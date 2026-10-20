//! `xmtp-bridge`: relays messages between people on messaging networks
//! (Signal, the web3 shoutbox) and one XMTP agent, with token-bucket rate
//! limiting.
//!
//! ```text
//! Signal users   ⇄ [Signal front end]                        ⇄ router (limits, sessions) ⇄ [xmtp-bridge-agent] ⇄ XMTP agent
//! shoutbox rooms ⇄ [xmtp-bridge-room] ⇄ [shoutbox front end] ⇄
//!                   child process        this process           this process                 child process
//! ```
//!
//! Nothing here listens on a port: Signal and XMTP are outbound connections,
//! and the workers are reached over pipes.

mod config;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context;
use bridge_core::message::{Frontend, FrontendIo};
use bridge_core::pacer::Pacer;
use bridge_core::router::Router;
use bridge_core::runtime::{RouterChannels, run_pacer, run_router};
use bridge_core::sessions::SessionStore;
use bridge_core::worker::{WorkerSpec, supervise};
use clap::{Parser, Subcommand};
use frontend_shoutbox::ShoutboxFrontend;
use frontend_signal::SignalFrontend;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use crate::config::Config;

/// Relay between messaging networks and an XMTP agent.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Config file.
    #[arg(short, long, default_value = "xmtp-bridge.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Link the bridge to its Signal account: scan the printed QR code from
    /// the account's phone (Settings → Linked devices).
    LinkSignal,
    /// Check the config, secret files and worker binary, then exit.
    Check,
    /// Run the bridge.
    Run,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    match cli.command {
        // presage runs its own single-threaded runtime.
        Cmd::LinkSignal => {
            config.prepare_dirs()?;
            frontend_signal::link_device(config.signal_settings()?)
        }
        Cmd::Check => {
            config.check()?;
            println!("config ok");
            Ok(())
        }
        Cmd::Run => tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(run(config)),
    }
}

async fn run(config: Config) -> anyhow::Result<()> {
    config.check()?;
    config.prepare_dirs()?;
    let sessions = SessionStore::load(&config.state_path, config.limits.max_sessions)?;
    tracing::info!(sessions = sessions.len(), "loaded sessions");
    let router = Router::new(config.router_config(), sessions, Instant::now());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (frontend_tx, frontend_rx) = mpsc::channel(1024);
    let (request_tx, request_rx) = mpsc::channel(1024);
    let (input_tx, input_rx) = mpsc::channel(1024);
    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();
    let mut outbound = HashMap::new();

    let mut frontends: Vec<Box<dyn Frontend>> = Vec::new();
    if config.signal.is_some() {
        frontends.push(Box::new(SignalFrontend::new(config.signal_settings()?)));
    }
    if config.shoutbox.is_some() {
        frontends.push(Box::new(ShoutboxFrontend::new(config.shoutbox_settings()?)));
    }
    anyhow::ensure!(!frontends.is_empty(), "no front end configured (add a [signal] or [shoutbox] section)");
    for frontend in frontends {
        let network = frontend.network().to_owned();
        let (pacer_tx, pacer_rx) = mpsc::channel(1024);
        let (deliver_tx, deliver_rx) = mpsc::channel(16);
        let pacer = Pacer::new(config.outbound_limits(), Instant::now());
        let reports = frontend_tx.clone();
        let stop = shutdown_rx.clone();
        tasks.spawn(async move {
            run_pacer(pacer, pacer_rx, deliver_tx, reports, stop).await;
            Ok(())
        });
        tasks.spawn(frontend.run(FrontendIo {
            events: frontend_tx.clone(),
            outbound: deliver_rx,
            shutdown: shutdown_rx.clone(),
        }));
        outbound.insert(network, pacer_tx);
    }
    drop(frontend_tx);

    let (program, args) = config.worker_command()?;
    tasks.spawn(supervise(WorkerSpec { program, args }, request_rx, input_tx, shutdown_rx.clone()));

    let channels =
        RouterChannels { frontend_events: frontend_rx, agent_inputs: input_rx, agent_requests: request_tx, outbound };
    let mut router_task = tokio::spawn(run_router(router, channels, config.state_path.clone(), shutdown_rx));

    let mut failure = None;
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            tracing::info!("shutting down");
        }
        Some(done) = tasks.join_next() => {
            let error = match done {
                Ok(Ok(())) => anyhow::anyhow!("a component stopped unexpectedly"),
                Ok(Err(error)) => error,
                Err(join) => anyhow::Error::new(join).context("a component panicked"),
            };
            tracing::error!(error = %format!("{error:#}"), "stopping the bridge");
            failure = Some(error);
        }
        router = &mut router_task => {
            router.context("router panicked")??;
            anyhow::bail!("router stopped unexpectedly");
        }
    }
    let _ = shutdown_tx.send(true);
    router_task.await.context("router panicked")?.context("saving sessions")?;
    let _ = tokio::time::timeout(Duration::from_secs(15), async { while tasks.join_next().await.is_some() {} }).await;
    failure.map_or(Ok(()), Err)
}

async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = term.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
}

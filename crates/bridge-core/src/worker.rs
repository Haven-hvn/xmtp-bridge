//! Runs a JSON-lines worker as a child process and restarts it when it exits.
//!
//! Both XMTP workers (`xmtp-bridge-agent`, `xmtp-bridge-room`) are driven
//! this way. The supervisor talks to the child over its stdin/stdout pipes
//! (one JSON object per line each way); its stderr is inherited so its logs
//! land next to the bridge's. Every spawn gets a new generation number:
//! requests tagged for another generation are dropped (the owner re-sends
//! whatever it still needs once the new worker is ready), so a request is
//! never replayed into a worker that already got it.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

const FIRST_RETRY: Duration = Duration::from_secs(5);
const MAX_RETRY: Duration = Duration::from_secs(300);
/// A worker that ran this long counts as healthy; the backoff resets.
const HEALTHY_RUN: Duration = Duration::from_secs(120);
const STOP_GRACE: Duration = Duration::from_secs(10);

/// An event type a worker writes on stdout.
pub trait WorkerEvent: DeserializeOwned + Send + 'static {
    /// Whether this event announces that the worker accepts requests.
    fn is_ready(&self) -> bool;
}

/// How to start a worker.
#[derive(Debug, Clone)]
pub struct WorkerSpec {
    /// Binary.
    pub program: PathBuf,
    /// Arguments.
    pub args: Vec<OsString>,
}

/// A request tagged with the worker generation it is meant for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tagged<Req> {
    /// Worker process generation.
    pub generation: u64,
    /// The request.
    pub request: Req,
}

/// What the supervisor reports to the worker's owner. Each worker process
/// gets a new `generation`, so nothing from (or meant for) a dead worker is
/// confused with its replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerInput<Ev> {
    /// A protocol event from worker `generation`.
    Event {
        /// Worker process generation.
        generation: u64,
        /// The event.
        event: Ev,
    },
    /// Worker `generation` exited.
    Down {
        /// Worker process generation.
        generation: u64,
    },
}

enum Outcome {
    Shutdown,
    Exited(String),
}

/// Supervise a worker until shutdown. Never gives up: an unreachable network
/// is retried with capped exponential backoff while the owner treats the
/// worker as down.
///
/// # Errors
/// None today; the signature leaves room for fatal setup errors.
pub async fn supervise<Req, Ev>(
    spec: WorkerSpec,
    mut requests: mpsc::Receiver<Tagged<Req>>,
    inputs: mpsc::Sender<WorkerInput<Ev>>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    Req: Serialize + Send + 'static,
    Ev: WorkerEvent,
{
    let mut generation = 0_u64;
    let mut retry = FIRST_RETRY;
    loop {
        generation += 1;
        let started = Instant::now();
        let outcome = run_worker(&spec, generation, &mut requests, &inputs, &mut shutdown).await;
        let _ = inputs.send(WorkerInput::Down { generation }).await;
        let reason = match outcome {
            Outcome::Shutdown => return Ok(()),
            Outcome::Exited(reason) => reason,
        };
        if started.elapsed() >= HEALTHY_RUN {
            retry = FIRST_RETRY;
        }
        tracing::warn!(worker = %spec.program.display(), generation, %reason, retry_in = ?retry, "worker stopped");
        let wake = tokio::time::sleep(retry);
        tokio::pin!(wake);
        loop {
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                () = &mut wake => break,
                // Requests cannot arrive while no worker is ready, but drain
                // defensively so the channel never fills.
                request = requests.recv() => if request.is_none() { return Ok(()) },
            }
        }
        retry = (retry * 2).min(MAX_RETRY);
    }
}

async fn run_worker<Req: Serialize, Ev: WorkerEvent>(
    spec: &WorkerSpec,
    generation: u64,
    requests: &mut mpsc::Receiver<Tagged<Req>>,
    inputs: &mpsc::Sender<WorkerInput<Ev>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Outcome {
    let mut child = match Command::new(&spec.program)
        .args(&spec.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => return Outcome::Exited(format!("spawning {}: {error}", spec.program.display())),
    };
    let (Some(mut stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Outcome::Exited("worker pipes unavailable".into());
    };
    let mut lines = BufReader::new(stdout).lines();
    let mut ready = false;
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                drop(stdin); // EOF on stdin: the worker exits cleanly
                stop(&mut child).await;
                return Outcome::Shutdown;
            }
            line = lines.next_line() => match line {
                Ok(Some(line)) => match serde_json::from_str::<Ev>(&line) {
                    Ok(event) => {
                        ready |= event.is_ready();
                        if inputs.send(WorkerInput::Event { generation, event }).await.is_err() {
                            stop(&mut child).await;
                            return Outcome::Shutdown;
                        }
                    }
                    Err(error) => tracing::warn!(%error, "malformed line from a worker"),
                },
                Ok(None) => {
                    let status = child.wait().await;
                    return Outcome::Exited(format!("worker exited ({status:?})"));
                }
                Err(error) => {
                    stop(&mut child).await;
                    return Outcome::Exited(format!("reading worker output: {error}"));
                }
            },
            request = requests.recv() => {
                let Some(Tagged { generation: target, request }) = request else {
                    stop(&mut child).await;
                    return Outcome::Shutdown;
                };
                if !ready || target != generation {
                    continue; // stale; the owner re-sends after Ready
                }
                let Ok(mut line) = serde_json::to_string(&request) else { continue };
                line.push('\n');
                if let Err(error) = stdin.write_all(line.as_bytes()).await {
                    stop(&mut child).await;
                    return Outcome::Exited(format!("writing to the worker: {error}"));
                }
                if let Err(error) = stdin.flush().await {
                    stop(&mut child).await;
                    return Outcome::Exited(format!("writing to the worker: {error}"));
                }
            }
        }
    }
}

/// Give the worker a moment to exit on its own, then kill it.
async fn stop(child: &mut Child) {
    if tokio::time::timeout(STOP_GRACE, child.wait()).await.is_err() {
        let _ = child.kill().await;
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::protocol::{AgentEvent, AgentRequest};

    /// A stand-in worker: announces Ready, answers the first request with
    /// `send_ok` for that request's id, then waits for stdin to close.
    fn fake_worker() -> WorkerSpec {
        let script = r#"
            echo '{"ev":"ready","inbox_id":"own","address":"0x1","agent_inbox_id":"agent"}'
            read line
            req=$(printf '%s' "$line" | sed 's/.*"req":\([0-9]*\).*/\1/')
            echo "{\"ev\":\"send_ok\",\"req\":$req}"
            cat > /dev/null
        "#;
        WorkerSpec { program: "/bin/sh".into(), args: vec!["-c".into(), script.into()] }
    }

    async fn next(rx: &mut mpsc::Receiver<WorkerInput<AgentEvent>>) -> WorkerInput<AgentEvent> {
        tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("timely").expect("open")
    }

    #[tokio::test]
    async fn forwards_current_generation_only_and_stops_cleanly() {
        let (req_tx, req_rx) = mpsc::channel(8);
        let (in_tx, mut in_rx) = mpsc::channel(8);
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(supervise(fake_worker(), req_rx, in_tx, stop_rx));

        let ready = next(&mut in_rx).await;
        assert!(matches!(ready, WorkerInput::Event { generation: 1, event: AgentEvent::Ready { .. } }));

        let send = |generation, req| Tagged {
            generation,
            request: AgentRequest::Send { req, thread: "t".into(), text: "x".into() },
        };
        req_tx.send(send(99, 1)).await.unwrap(); // stale generation: dropped
        req_tx.send(send(1, 7)).await.unwrap();
        assert_eq!(next(&mut in_rx).await, WorkerInput::Event { generation: 1, event: AgentEvent::SendOk { req: 7 } });

        stop_tx.send(true).unwrap();
        assert_eq!(next(&mut in_rx).await, WorkerInput::Down { generation: 1 });
        tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap().unwrap();
    }
}

//! Async drivers: the router loop and the per-network outbound pacer loop.
//!
//! Channel discipline (prevents a send cycle from deadlocking): the router
//! only ever awaits the pacer input and the worker-request channel, and both
//! of those consumers drain their input continuously. The pacer forwards to
//! the front end only when the front end's channel has room, and reports
//! drops back with `try_send`, so it never blocks on the router.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use crate::message::{FrontendEvent, Outbound};
use crate::pacer::Pacer;
use crate::protocol::AgentRequest;
use crate::router::{Action, AgentInput, Router};

/// An agent-worker request tagged with the worker generation it is meant for.
pub type Tagged = crate::worker::Tagged<AgentRequest>;

/// Channels the router loop runs over.
#[derive(Debug)]
pub struct RouterChannels {
    /// Front-end events (all networks) and pacer drop reports.
    pub frontend_events: mpsc::Receiver<FrontendEvent>,
    /// Worker supervisor input.
    pub agent_inputs: mpsc::Receiver<AgentInput>,
    /// Requests to the worker supervisor.
    pub agent_requests: mpsc::Sender<Tagged>,
    /// Pacer input per network id.
    pub outbound: HashMap<String, mpsc::Sender<Outbound>>,
}

/// How often unsaved session changes are written.
pub const SAVE_INTERVAL: Duration = Duration::from_secs(2);

/// Run the router until shutdown, persisting sessions to `state_path`.
///
/// # Errors
/// A failed final save.
pub async fn run_router(
    mut router: Router,
    mut ch: RouterChannels,
    state_path: PathBuf,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut save_tick = tokio::time::interval(SAVE_INTERVAL);
    loop {
        let actions = tokio::select! {
            Some(event) = ch.frontend_events.recv() => router.on_frontend(event, Instant::now()),
            Some(input) = ch.agent_inputs.recv() => router.on_agent(input, Instant::now()),
            _ = save_tick.tick() => {
                if router.sessions_mut().is_dirty()
                    && let Err(error) = router.sessions_mut().save(&state_path)
                {
                    tracing::error!(error = %format!("{error:#}"), "saving sessions failed");
                }
                continue;
            }
            _ = shutdown.changed() => break,
            else => break,
        };
        for action in actions {
            match action {
                Action::Agent(request) => {
                    // The router emits requests only while a worker is ready.
                    let Some(generation) = router.agent_generation() else { continue };
                    if ch.agent_requests.send(Tagged { generation, request }).await.is_err() {
                        tracing::error!("agent supervisor stopped");
                    }
                }
                Action::Frontend(message) => match ch.outbound.get(&message.to.network) {
                    Some(pacer) => {
                        if pacer.send(message).await.is_err() {
                            tracing::error!("outbound pacer stopped");
                        }
                    }
                    None => tracing::warn!(network = %message.to.network, "no front end for network"),
                },
            }
        }
    }
    router.sessions_mut().save(&state_path)
}

/// Run one network's pacer until shutdown or until its input closes.
pub async fn run_pacer(
    mut pacer: Pacer,
    mut input: mpsc::Receiver<Outbound>,
    output: mpsc::Sender<Outbound>,
    reports: mpsc::Sender<FrontendEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let wait = pacer.next_ready_in(Instant::now());
        let sendable = wait == Some(Duration::ZERO);
        let sleep_for = wait.filter(|d| !d.is_zero());
        tokio::select! {
            _ = shutdown.changed() => return,
            message = input.recv() => {
                let Some(message) = message else { return };
                if let Err(dropped) = pacer.push(message) {
                    tracing::warn!(to = %dropped.to, "outbound queue full; dropping message");
                    if let Some(ack) = dropped.ack {
                        let reason = "outbound queue full".to_owned();
                        let _ = reports.try_send(FrontendEvent::Failed { ack, reason });
                    }
                }
            }
            permit = output.reserve(), if sendable => {
                let Ok(permit) = permit else { return };
                if let Some(message) = pacer.pop_ready(Instant::now()) {
                    permit.send(message);
                }
            }
            () = tokio::time::sleep(sleep_for.unwrap_or_default()), if sleep_for.is_some() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::Rate;
    use crate::message::UserAddress;
    use crate::pacer::OutboundLimits;

    #[tokio::test]
    async fn pacer_loop_forwards_in_order_and_reports_overflow() {
        let limits = OutboundLimits {
            per_recipient: Rate::new(10, 10, 1),
            global: Rate::new(10, 10, 1),
            max_queue_per_recipient: 2,
            max_queue_total: 2,
        };
        let (in_tx, in_rx) = mpsc::channel(8);
        // Front end channel of 1 and nobody reading yet: the queue fills.
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let (rep_tx, mut rep_rx) = mpsc::channel(8);
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(run_pacer(Pacer::new(limits, Instant::now()), in_rx, out_tx, rep_tx, stop_rx));

        let to = UserAddress::new("signal", "a");
        for i in 0..4 {
            let ack = crate::message::DeliveryAck { thread: "t".into(), message_id: i.to_string(), sent_at_ns: i };
            in_tx.send(Outbound { to: to.clone(), text: i.to_string(), ack: Some(ack) }).await.unwrap();
        }
        // At most one message fits in the front-end channel and two in the
        // queue, so at least one of the four overflows. Which one depends on
        // scheduling; the invariants below do not.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut dropped = Vec::new();
        while let Ok(FrontendEvent::Failed { ack, .. }) = rep_rx.try_recv() {
            dropped.push(ack.message_id);
        }
        assert!(!dropped.is_empty(), "overflow is reported");
        let mut delivered = Vec::new();
        while let Ok(Some(message)) = tokio::time::timeout(Duration::from_millis(200), out_rx.recv()).await {
            delivered.push(message.text);
        }
        assert_eq!(delivered.len() + dropped.len(), 4, "every message is delivered or reported");
        let mut sorted = delivered.clone();
        sorted.sort();
        assert_eq!(delivered, sorted, "delivery keeps order");
        assert!(delivered.iter().all(|d| !dropped.contains(d)));
        stop_tx.send(true).unwrap();
        task.await.unwrap();
    }
}

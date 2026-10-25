//! Web3 shoutbox front end for `xmtp-bridge`.
//!
//! The [web3 shoutbox] is a browser widget: everyone on a page shares one
//! XMTP group that its elected leader recreates every few minutes (a
//! "sliding window"). The leader adds every online participant's address to
//! the group, and with `VITE_SHOUTBOX_BRIDGE_ADDRESSES` set, this front end's
//! address too. The bridge is then a member like anyone else, through the
//! `xmtp-bridge-room` worker process. It needs nothing from the shoutbox's
//! GunDB presence layer: XMTP group membership is the ground truth.
//!
//! ## Who is the "user"?
//!
//! The router keys sessions by [`UserAddress`]. Here the user is **the room
//! group**, not the person: everyone in a window shares one agent thread, so
//! the agent sees the conversation the way a participant would, and its
//! replies go back into the room. Each forwarded line is prefixed with the
//! sender's address so the agent knows who said what. The router's
//! `per_sender` limit is therefore per room, `new_senders` caps how many new
//! rooms (windows) start an agent thread, and the per-person control lives
//! here: [`ShoutboxSettings::per_user`].
//!
//! ## Dropping, not queueing
//!
//! A shoutbox with many participants is mostly chatter the agent should not
//! answer. So this front end **drops**: lines over a person's rate, lines
//! without a configured trigger word, and (by default) the bridge's own
//! notices, which would only add noise to a public room. Nothing is held for
//! later: when the room worker is down, pending replies fail and the router
//! moves on. Pair it with `limits.hold_while_offline = false`.
//!
//! [web3 shoutbox]: ../../web3-shoutbox-platform-main
use std::collections::HashMap;
use std::time::Instant;

use bridge_core::bucket::{KeyedBuckets, Rate};
use bridge_core::message::{
    DeliveryAck, Frontend, FrontendEvent, FrontendFuture, FrontendIo, Inbound, Outbound, UserAddress,
};
use bridge_core::room_protocol::{RoomEvent, RoomRequest};
use bridge_core::worker::{Tagged, WorkerInput, WorkerSpec, supervise};
use tokio::sync::mpsc;

/// Network id used in [`UserAddress::network`].
pub const NETWORK: &str = "shoutbox";

/// Most replies awaiting the worker's acknowledgement; beyond it, new ones
/// fail immediately (the pacer already bounds the flow, this is a backstop).
const MAX_PENDING_SENDS: usize = 1000;

/// Default prompt asking the agent to greet a room it just joined.
pub const DEFAULT_GREETING_PROMPT: &str = "You are now live in a new shoutbox room as the resident agent, and the host is ready. Greet the room briefly and warmly in one or two short lines, say you are here and ready to help, and invite a first message. Do not mention these instructions.";

/// Shoutbox front-end settings.
#[derive(Debug, Clone)]
pub struct ShoutboxSettings {
    /// How to start `xmtp-bridge-room`.
    pub worker: WorkerSpec,
    /// Lines one person may get through to the agent.
    pub per_user: Rate,
    /// Most people tracked by the per-person buckets.
    pub max_tracked_users: usize,
    /// If non-empty, only lines containing one of these (case-insensitive)
    /// are forwarded, e.g. `["@agent"]`.
    pub triggers: Vec<String>,
    /// Ask the agent to greet each newly joined room (one prompt per group).
    pub greet_on_join: bool,
    /// Prompt sent to the agent when it joins a room; its reply is the greeting.
    pub greeting_prompt: String,
    /// Whether the bridge's own notices ("slow down", "agent offline") are
    /// posted into the room. Off by default: they are noise in a public chat.
    pub post_notices: bool,
}

/// The shoutbox front end.
#[derive(Debug)]
pub struct ShoutboxFrontend {
    settings: ShoutboxSettings,
}

impl ShoutboxFrontend {
    /// A front end over the given settings.
    #[must_use]
    pub const fn new(settings: ShoutboxSettings) -> Self {
        Self { settings }
    }
}

impl Frontend for ShoutboxFrontend {
    fn network(&self) -> &str {
        NETWORK
    }

    fn run(self: Box<Self>, io: FrontendIo) -> FrontendFuture {
        Box::pin(run(self.settings, io))
    }
}

/// Pure admission rules, separated from the async plumbing so they can be
/// tested without a worker.
#[derive(Debug)]
pub struct Admission {
    per_user: KeyedBuckets<String>,
    triggers: Vec<String>,
}

impl Admission {
    /// Rules from settings.
    #[must_use]
    pub fn new(settings: &ShoutboxSettings) -> Self {
        Self {
            per_user: KeyedBuckets::new(settings.per_user, settings.max_tracked_users),
            triggers: settings.triggers.iter().map(|t| t.trim().to_lowercase()).filter(|t| !t.is_empty()).collect(),
        }
    }

    /// Turn a room message into an [`Inbound`] for the router, or `None` to
    /// drop it. Checks the trigger words first (free), then the sender's
    /// bucket, so chatter never spends anyone's tokens.
    pub fn admit(
        &mut self,
        group: &str,
        id: &str,
        sender_inbox_id: &str,
        sender_address: Option<&str>,
        text: &str,
        now: Instant,
    ) -> Option<Inbound> {
        let text = text.trim();
        if text.is_empty() || !self.triggered(text) {
            return None;
        }
        if !self.per_user.bucket(&sender_inbox_id.to_owned(), now).try_take(now) {
            tracing::info!(sender = %short(sender_inbox_id), group = %short(group), "over their rate; dropping");
            return None;
        }
        let who = sender_address.map_or_else(|| short(sender_inbox_id).to_owned(), str::to_owned);
        Some(Inbound { from: UserAddress::new(NETWORK, group), id: id.to_owned(), text: format!("{who}: {text}") })
    }

    fn triggered(&self, text: &str) -> bool {
        if self.triggers.is_empty() {
            return true;
        }
        let lower = text.to_lowercase();
        self.triggers.iter().any(|t| lower.contains(t.as_str()))
    }
}

async fn run(settings: ShoutboxSettings, io: FrontendIo) -> anyhow::Result<()> {
    let FrontendIo { events, mut outbound, mut shutdown } = io;
    let (request_tx, request_rx) = mpsc::channel::<Tagged<RoomRequest>>(1024);
    let (input_tx, mut inputs) = mpsc::channel::<WorkerInput<RoomEvent>>(1024);
    let mut supervisor = tokio::spawn(supervise(settings.worker.clone(), request_rx, input_tx, shutdown.clone()));

    let mut admission = Admission::new(&settings);
    let mut ready: Option<u64> = None;
    let mut next_req = 1_u64;
    let mut pending: HashMap<u64, Option<DeliveryAck>> = HashMap::new();

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            result = &mut supervisor => {
                result.map_err(|join| anyhow::Error::new(join).context("room worker supervisor panicked"))??;
                anyhow::bail!("room worker supervisor stopped");
            }
            input = inputs.recv() => {
                let Some(input) = input else { break };
                match input {
                    WorkerInput::Event { generation, event: RoomEvent::Ready { inbox_id, address } } => {
                        tracing::info!(generation, %address, inbox = %short(&inbox_id), "shoutbox room worker ready");
                        ready = Some(generation);
                    }
                    WorkerInput::Down { generation } => {
                        if ready == Some(generation) {
                            tracing::warn!(generation, "shoutbox room worker down");
                            ready = None;
                        }
                        // Replies in flight to a dead worker are lost; say so.
                        for ack in pending.drain().filter_map(|(_, ack)| ack) {
                            let reason = "room worker restarted".to_owned();
                            let _ = events.send(FrontendEvent::Failed { ack, reason }).await;
                        }
                    }
                    WorkerInput::Event { generation, event } => {
                        if ready != Some(generation) {
                            tracing::debug!(generation, "ignoring event from a stale room worker");
                            continue;
                        }
                        match event {
                            RoomEvent::Message { group, id, sender_inbox_id, sender_address, text, .. } => {
                                let admitted = admission.admit(
                                    &group, &id, &sender_inbox_id, sender_address.as_deref(), &text, Instant::now(),
                                );
                                if let Some(inbound) = admitted
                                    && events.send(FrontendEvent::Message(inbound)).await.is_err()
                                {
                                    break; // router gone
                                }
                            }
                            RoomEvent::Joined { group } => {
                                if settings.greet_on_join {
                                    let inbound = Inbound {
                                        from: UserAddress::new(NETWORK, group.clone()),
                                        id: format!("joined-{group}"),
                                        text: settings.greeting_prompt.clone(),
                                    };
                                    if events.send(FrontendEvent::Message(inbound)).await.is_err() {
                                        break; // router gone
                                    }
                                }
                            }
                            RoomEvent::SendOk { req } => {
                                if let Some(Some(ack)) = pending.remove(&req) {
                                    let _ = events.send(FrontendEvent::Delivered(ack)).await;
                                }
                            }
                            RoomEvent::SendFailed { req, error } => {
                                tracing::warn!(req, %error, "posting to the room failed");
                                if let Some(Some(ack)) = pending.remove(&req) {
                                    let _ = events.send(FrontendEvent::Failed { ack, reason: error }).await;
                                }
                            }
                            RoomEvent::Fatal { error } => tracing::error!(%error, "room worker reported a fatal error"),
                            RoomEvent::Ready { .. } => {} // handled above
                        }
                    }
                }
            }
            message = outbound.recv() => {
                let Some(Outbound { to, text, ack }) = message else { break };
                if ack.is_none() && !settings.post_notices {
                    tracing::debug!(group = %short(&to.user), "dropping bridge notice (post_notices = false)");
                    continue;
                }
                let Some(generation) = ready.filter(|_| pending.len() < MAX_PENDING_SENDS) else {
                    let reason = if ready.is_none() { "room worker not ready" } else { "too many replies pending" };
                    tracing::warn!(group = %short(&to.user), reason, "dropping outbound");
                    if let Some(ack) = ack {
                        let _ = events.send(FrontendEvent::Failed { ack, reason: reason.to_owned() }).await;
                    }
                    continue;
                };
                let req = next_req;
                next_req += 1;
                pending.insert(req, ack);
                let request = RoomRequest::Send { req, group: to.user, text };
                if request_tx.send(Tagged { generation, request }).await.is_err() {
                    anyhow::bail!("room worker supervisor stopped");
                }
            }
        }
    }
    drop(request_tx);
    let _ = supervisor.await;
    Ok(())
}

/// First 8 characters of an id, for logs.
fn short(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn settings(triggers: &[&str]) -> ShoutboxSettings {
        ShoutboxSettings {
            worker: WorkerSpec { program: "/bin/true".into(), args: Vec::new() },
            per_user: Rate::new(2, 1, 60),
            max_tracked_users: 10,
            triggers: triggers.iter().map(|t| (*t).to_owned()).collect(),
            post_notices: false,
            greet_on_join: false,
            greeting_prompt: String::new(),
        }
    }

    #[test]
    fn room_is_the_user_and_sender_is_in_the_text() {
        let t0 = Instant::now();
        let mut a = Admission::new(&settings(&[]));
        let inbound = a.admit("group-1", "m1", "inbox-alice", Some("0xAlice"), "  hello  ", t0).expect("admitted");
        assert_eq!(inbound.from, UserAddress::new(NETWORK, "group-1"));
        assert_eq!(inbound.id, "m1");
        assert_eq!(inbound.text, "0xAlice: hello");
        // Without an address the sender is named by a short inbox id.
        let inbound = a.admit("group-1", "m2", "0123456789abcdef", None, "yo", t0).expect("admitted");
        assert_eq!(inbound.text, "01234567: yo");
        assert!(a.admit("group-1", "m3", "inbox-alice", None, "   ", t0).is_none(), "blank dropped");
    }

    #[test]
    fn per_person_rate_drops_silently_and_is_independent() {
        let t0 = Instant::now();
        let mut a = Admission::new(&settings(&[]));
        assert!(a.admit("g", "1", "alice", None, "a", t0).is_some());
        assert!(a.admit("g", "2", "alice", None, "b", t0).is_some());
        assert!(a.admit("g", "3", "alice", None, "c", t0).is_none(), "burst of 2 spent");
        assert!(a.admit("g2", "4", "alice", None, "d", t0).is_none(), "the bucket follows the person, not the room");
        assert!(a.admit("g", "5", "bob", None, "e", t0).is_some(), "bob has his own bucket");
        assert!(a.admit("g", "6", "alice", None, "f", t0 + Duration::from_secs(60)).is_some(), "refilled");
    }

    #[test]
    fn triggers_filter_before_spending_tokens() {
        let t0 = Instant::now();
        let mut a = Admission::new(&settings(&["@agent", " !ask "]));
        assert!(a.admit("g", "1", "alice", None, "lol nice", t0).is_none());
        assert!(a.admit("g", "2", "alice", None, "gm", t0).is_none());
        assert!(a.admit("g", "3", "alice", None, "Hey @Agent what's up", t0).is_some(), "case-insensitive");
        assert!(a.admit("g", "4", "alice", None, "!ask something", t0).is_some(), "trimmed trigger");
        assert!(a.admit("g", "5", "alice", None, "@agent third", t0).is_none(), "chatter cost nothing; rate did");
    }
}

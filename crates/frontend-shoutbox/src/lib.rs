//! Web3 shoutbox front end for `xmtp-bridge`.
//!
//! The [web3 shoutbox] is a browser widget: everyone on a page shares one
//! XMTP group that its elected leader recreates every few minutes (a
//! "sliding window"). The leader adds every online participant's address to
//! the group, and with `VITE_SHOUTBOX_BRIDGE_ADDRESSES` set, this front end's
//! address too. The bridge is then a member like anyone else, through the
//! `xmtp-bridge-room` worker process. Relaying chat needs nothing from the
//! shoutbox's GunDB presence layer — XMTP group membership is the ground
//! truth — but the join greeting does, so the agent never announces itself
//! to an empty room: see [`PresenceGate`].
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
//! ## Which rooms?
//!
//! A room group only ever says which room it is through its XMTP name — the
//! frontend writes the normalized room URL there when its leader creates the
//! group, and the room worker reports it back on every event. When
//! [`ShoutboxSettings::allowed_rooms`] is set, anything else is dropped
//! before it can reach the router, and no join greeting is sent: see
//! [`Admission::room_allowed`]. That matters because the bridge is a public
//! address anyone can add to a group of their own, and because the frontend
//! has more than one way to end up in a room nobody meant to open.
//!
//! ## Who is in the room?
//!
//! [`ShoutboxSettings::greet_on_join`] makes the agent announce itself in
//! every room it joins — one prompt per room, plus one per rejoin after a
//! bridge restart — whether or not anyone is there. With
//! [`ShoutboxSettings::greet_presence_file`] set, [`PresenceGate`] spends
//! those prompts only while a human is actually present: the deployment's
//! `bridge-presence.mjs` mirrors live GunDB heartbeats into a snapshot
//! file, and a missing, unreadable or stale snapshot means no greeting.
//! Left unset, the gate is off and every allowed room is greeted as
//! before.
//!
//! [web3 shoutbox]: ../../web3-shoutbox-platform-main
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bridge_core::bucket::{KeyedBuckets, Rate};
use bridge_core::message::{
    DeliveryAck, Frontend, FrontendEvent, FrontendFuture, FrontendIo, Inbound, Outbound, UserAddress,
};
use bridge_core::room_protocol::{RoomEvent, RoomRequest};
use bridge_core::worker::{Tagged, WorkerInput, WorkerSpec, supervise};
use sha2::{Digest, Sha256};
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
    /// Presence snapshot the join greeting is gated on; `None` (the
    /// default) greets every allowed room whether or not anyone is in it.
    pub greet_presence_file: Option<PathBuf>,
    /// How young a snapshot timestamp must be to count as a human online.
    pub greet_presence_ttl: Duration,
    /// Whether the bridge's own notices ("slow down", "agent offline") are
    /// posted into the room. Off by default: they are noise in a public chat.
    pub post_notices: bool,
    /// Room names the bridge serves, exactly as the frontend stamps them
    /// into a group's XMTP name (the normalized room URL). Empty means
    /// every room, which is the default so an unconfigured bridge keeps its
    /// old behavior; otherwise only an exact match is served and every other
    /// group — including unnamed ones — is ignored.
    pub allowed_rooms: Vec<String>,
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
    allowed_rooms: std::collections::HashSet<String>,
}

impl Admission {
    /// Rules from settings.
    #[must_use]
    pub fn new(settings: &ShoutboxSettings) -> Self {
        Self {
            per_user: KeyedBuckets::new(settings.per_user, settings.max_tracked_users),
            triggers: settings.triggers.iter().map(|t| t.trim().to_lowercase()).filter(|t| !t.is_empty()).collect(),
            allowed_rooms: settings.allowed_rooms.iter().cloned().collect(),
        }
    }

    /// Whether the bridge serves the room a group belongs to.
    ///
    /// A group says which room it is only through its XMTP name: the
    /// frontend's leader writes the normalized room URL into the name when
    /// it creates the group, and the room worker reports it back here. The
    /// room key itself never reaches the bridge, and group ids rotate with
    /// every sliding window, so the name is the one stable thing to match.
    ///
    /// This is a policy filter, not a security boundary — XMTP group names
    /// are unsigned, so anyone can create a group named like an allowed one
    /// and add the bridge's public address to it. It exists to stop rooms
    /// being opened by accident.
    #[must_use]
    pub fn room_allowed(&self, group_name: Option<&str>) -> bool {
        self.allowed_rooms.is_empty() || group_name.is_some_and(|name| self.allowed_rooms.contains(name))
    }

    /// Turn a room message into an [`Inbound`] for the router, or `None` to
    /// drop it. Checks the trigger words first (free), then the sender's
    /// bucket, so chatter never spends anyone's tokens.
    ///
    /// The room allowlist is deliberately *not* applied here: it is a gate
    /// on the room, checked once per event before this, so a disallowed room
    /// costs no bucket lookup and cannot spend a sender's tokens.
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

/// Version stamped into every presence snapshot this bridge understands.
const PRESENCE_SNAPSHOT_VERSION: u32 = 1;

/// Gate for the join greeting on actual human presence.
///
/// The frontend keeps a GunDB heartbeat per participant — fresh for 30 s,
/// rewritten every 10 — and the Rust bridge never talks to Gun. The
/// deployment's `bridge-presence.mjs`, which already runs a Gun client to
/// keep its own heartbeat alive, mirrors those heartbeats into a JSON
/// snapshot, and this gate reads that file at the moment the bridge joins
/// a room. The snapshot's shape is [`PresenceSnapshot`].
///
/// With no file configured ([`ShoutboxSettings::greet_presence_file`])
/// the gate is off and every allowed room is greeted, as before. With one
/// configured, absence of evidence counts as absence of people: a
/// missing, unreadable or stale snapshot suppresses the greeting, so an
/// empty room never costs a prompt — and neither does a bridge restart
/// rejoining every window it remembers.
#[derive(Debug, Clone)]
pub struct PresenceGate {
    /// Snapshot file to read; `None` disables the gate.
    file: Option<PathBuf>,
    /// How young a timestamp in it must be to count as a human online.
    ttl: Duration,
}

impl PresenceGate {
    /// The gate `settings` describes.
    #[must_use]
    pub fn from_settings(settings: &ShoutboxSettings) -> Self {
        Self { file: settings.greet_presence_file.clone(), ttl: settings.greet_presence_ttl }
    }

    /// Whether the join greeting may go out for `group`.
    ///
    /// Silent when it allows, and silent when no gate is configured;
    /// every refusal logs its reason, so a room the agent stayed quiet in
    /// is always explainable from the log alone.
    #[must_use]
    pub fn allows_greeting(&self, group: &str, group_name: Option<&str>) -> bool {
        let Some(path) = &self.file else {
            return true; // no gate configured: greet as this bridge always did
        };
        let Some(room_name) = group_name else {
            // A nameless group cannot be matched to any room's heartbeats.
            tracing::info!(group = %short(group), "no room name to look up presence; not greeting");
            return false;
        };
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX));
        let ttl_ms = u64::try_from(self.ttl.as_millis()).unwrap_or(u64::MAX);
        let verdict = std::fs::read_to_string(path)
            .map_err(|error| anyhow::anyhow!("reading {}: {error}", path.display()))
            .and_then(|text| Self::fresh_in(&text, &room_key(room_name), now_ms, ttl_ms));
        match verdict {
            Ok(true) => true,
            Ok(false) => {
                tracing::info!(group = %short(group), name = %room_name, "no human presence in the room; not greeting");
                false
            }
            Err(error) => {
                tracing::warn!(group = %short(group), path = %path.display(), %error, "presence snapshot unusable; not greeting");
                false
            }
        }
    }

    /// Read a snapshot: does `room_key` hold a heartbeat younger than
    /// `ttl_ms` at `now_ms`?
    ///
    /// # Errors
    /// The snapshot is not JSON this bridge understands or carries a
    /// version it does not know. Either way the caller stays quiet,
    /// because guessing at a stale snapshot is the exact failure this
    /// gate exists to prevent.
    fn fresh_in(snapshot: &str, room_key: &str, now_ms: u64, ttl_ms: u64) -> anyhow::Result<bool> {
        let parsed: PresenceSnapshot = serde_json::from_str(snapshot)?;
        anyhow::ensure!(
            parsed.version == PRESENCE_SNAPSHOT_VERSION,
            "unsupported presence snapshot version {} (want {PRESENCE_SNAPSHOT_VERSION})",
            parsed.version
        );
        Ok(parsed.rooms.get(room_key).is_some_and(|ts| *ts > 0 && now_ms.saturating_sub(*ts) < ttl_ms))
    }
}

/// What `bridge-presence.mjs` writes: a versioned map from room key (the
/// frontend's GunDB key — sha256 hex of the normalized room URL, which is
/// exactly what it stamps into a group's XMTP name) to the unix-ms
/// timestamp of that room's freshest human heartbeat.
///
/// A room at `0` was tracked but has never had a human in it; a room
/// absent from the map was never seen at all. Both read as "empty", so a
/// snapshot that stopped updating simply ages out.
#[derive(Debug, serde::Deserialize)]
struct PresenceSnapshot {
    /// Format version; an unknown one is refused rather than guessed at.
    version: u32,
    /// Room key -> unix-ms of the freshest human heartbeat.
    #[serde(default)]
    rooms: HashMap<String, u64>,
}

/// The frontend's GunDB room key for `room_name`: sha256 hex of the
/// normalized room URL, so hashing the name back reproduces the key its
/// heartbeats live under. Like [`Admission::room_allowed`] this
/// deliberately does not re-normalize — matching what it is given is the
/// bridge's job, normalizing is the frontend's.
fn room_key(room_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(room_name.as_bytes());
    hex::encode(hasher.finalize())
}

async fn run(settings: ShoutboxSettings, io: FrontendIo) -> anyhow::Result<()> {
    let FrontendIo { events, mut outbound, mut shutdown } = io;
    let (request_tx, request_rx) = mpsc::channel::<Tagged<RoomRequest>>(1024);
    let (input_tx, mut inputs) = mpsc::channel::<WorkerInput<RoomEvent>>(1024);
    let mut supervisor = tokio::spawn(supervise(settings.worker.clone(), request_rx, input_tx, shutdown.clone()));

    let mut admission = Admission::new(&settings);
    let presence = PresenceGate::from_settings(&settings);
    let mut ready: Option<u64> = None;
    let mut next_req = 1_u64;
    // In-flight room posts, with the target group so a failed reply can
    // be answered with a failure notice in the right room.
    #[derive(Debug)]
    struct PendingSend {
        ack: Option<DeliveryAck>,
        group: String,
        is_notice: bool,
    }
    let mut pending: HashMap<u64, PendingSend> = HashMap::new();

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
                        for ack in pending.drain().filter_map(|(_, p)| p.ack) {
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
                            RoomEvent::Message { group, id, sender_inbox_id, sender_address, text, group_name, .. } => {
                                // Room gate first: an unsolicited room costs
                                // nothing at all — no bucket lookup, and no
                                // chance of spending the sender's tokens.
                                if !admission.room_allowed(group_name.as_deref()) {
                                    tracing::debug!(
                                        group = %short(&group),
                                        name = group_name.as_deref().unwrap_or("<unnamed>"),
                                        "room outside allowed_rooms; dropping"
                                    );
                                    continue;
                                }
                                let admitted = admission.admit(
                                    &group, &id, &sender_inbox_id, sender_address.as_deref(), &text, Instant::now(),
                                );
                                if let Some(inbound) = admitted
                                    && events.send(FrontendEvent::Message(inbound)).await.is_err()
                                {
                                    break; // router gone
                                }
                            }
                            RoomEvent::Joined { group, group_name } => {
                                if !admission.room_allowed(group_name.as_deref()) {
                                    tracing::info!(
                                        group = %short(&group),
                                        name = group_name.as_deref().unwrap_or("<unnamed>"),
                                        "joined a room outside allowed_rooms; not greeting"
                                    );
                                    continue;
                                }
                                // The presence gate has its own log line
                                // for why it stayed silent, so a quiet
                                // join is always explainable.
                                if settings.greet_on_join && presence.allows_greeting(&group, group_name.as_deref()) {
                                    tracing::info!(
                                        group = %short(&group),
                                        name = group_name.as_deref().unwrap_or("<unnamed>"),
                                        "human presence in the room; greeting"
                                    );
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
                                if let Some(done) = pending.remove(&req) {
                                    tracing::info!(req, group = %short(&done.group), "posted to room");
                                    if let Some(ack) = done.ack {
                                        let _ = events.send(FrontendEvent::Delivered(ack)).await;
                                    }
                                }
                            }
                            RoomEvent::SendFailed { req, error } => {
                                tracing::warn!(req, %error, "posting to the room failed");
                                if let Some(failed) = pending.remove(&req) {
                                    if let Some(ack) = failed.ack {
                                        let _ = events.send(FrontendEvent::Failed { ack, reason: error }).await;
                                    }
                                    // A failed reply must not be silent in
                                    // the room: one short failure notice,
                                    // never for a failed notice itself.
                                    if !failed.is_notice && let Some(generation) = ready {
                                        let notice_req = next_req;
                                        next_req += 1;
                                        pending.insert(notice_req, PendingSend {
                                            ack: None,
                                            group: failed.group.clone(),
                                            is_notice: true,
                                        });
                                        let request = RoomRequest::Send {
                                            req: notice_req,
                                            group: failed.group,
                                            text: "The agent's reply could not be posted here — please try again.".to_owned(),
                                        };
                                        if request_tx.send(Tagged { generation, request }).await.is_err() {
                                            anyhow::bail!("room worker supervisor stopped");
                                        }
                                    }
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
                tracing::info!(req, group = %short(&to.user), "posting reply to room");
                pending.insert(req, PendingSend { ack, group: to.user.clone(), is_notice: false });
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
    use super::*;

    fn settings(triggers: &[&str]) -> ShoutboxSettings {
        settings_in(triggers, &[])
    }

    fn settings_in(triggers: &[&str], allowed_rooms: &[&str]) -> ShoutboxSettings {
        ShoutboxSettings {
            worker: WorkerSpec { program: "/bin/true".into(), args: Vec::new() },
            per_user: Rate::new(2, 1, 60),
            max_tracked_users: 10,
            triggers: triggers.iter().map(|t| (*t).to_owned()).collect(),
            post_notices: false,
            greet_on_join: false,
            greeting_prompt: String::new(),
            greet_presence_file: None,
            greet_presence_ttl: Duration::from_secs(45),
            allowed_rooms: allowed_rooms.iter().map(|r| (*r).to_owned()).collect(),
        }
    }

    #[test]
    fn only_allowed_rooms_are_served() {
        const ROOM: &str = "https://shoutbox.pinme.dev/default";

        // No allowlist configured: the bridge keeps serving every room it
        // is added to, named or not.
        let open = Admission::new(&settings(&[]));
        assert!(open.room_allowed(None), "an unconfigured bridge serves unnamed rooms");
        assert!(open.room_allowed(Some("https://shoutbox.pinme.dev/general")));

        let pinned = Admission::new(&settings_in(&[], &[ROOM]));
        assert!(pinned.room_allowed(Some(ROOM)), "the room itself");
        assert!(!pinned.room_allowed(Some("https://shoutbox.pinme.dev/general")), "a room outside the list");
        assert!(
            !pinned.room_allowed(Some("https://shoutbox.pinme.dev/default/")),
            "exact match only: normalizing is the frontend's job, not the bridge's"
        );
        assert!(!pinned.room_allowed(None), "an unnamed group is not the allowed room");
        assert!(!pinned.room_allowed(Some("")), "an empty name is not a match");
    }

    /// Wall-clock milliseconds — the clock the snapshot writer runs on.
    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(0))
    }

    fn snapshot_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("xmtp-bridge-presence-{name}-{}.json", std::process::id()))
    }

    #[test]
    fn room_key_is_the_frontends_room_key() {
        assert_eq!(
            room_key("https://shoutbox.pinme.dev/default"),
            "f3a65fc278b0ed45ef55b5d7eb8870059b28555e8cbab040b050e8881aee3d91",
            "sha256 hex of the normalized room URL, exactly as the frontend hashes it"
        );
    }

    #[test]
    fn presence_gate_is_off_until_a_snapshot_is_configured() {
        let gate = PresenceGate::from_settings(&settings(&[]));
        assert!(gate.allows_greeting("group-1", Some("https://shoutbox.pinme.dev/default")));
        assert!(gate.allows_greeting("group-1", None), "no gate means no questions");
    }

    #[test]
    fn presence_gate_greets_only_while_a_human_heartbeat_is_fresh() {
        const ROOM: &str = "https://shoutbox.pinme.dev/default";
        let path = snapshot_path("gated");
        let key = room_key(ROOM);
        let now = now_ms();
        let gate = PresenceGate { file: Some(path.clone()), ttl: Duration::from_secs(45) };
        let write = |body: String| std::fs::write(&path, body).expect("write the snapshot");

        // A heartbeat from seconds ago: the room has someone in it.
        write(format!(r#"{{"version":1,"rooms":{{"{key}":{now}}}}}"#));
        assert!(gate.allows_greeting("g", Some(ROOM)), "a fresh heartbeat is a person");

        // One window past the TTL: the heartbeat is dead, so is the room.
        write(format!(r#"{{"version":1,"rooms":{{"{key}":{}}}}}"#, now - 46_000));
        assert!(!gate.allows_greeting("g", Some(ROOM)), "a stale heartbeat is an empty room");

        // Tracked but never visited, and never tracked at all: both empty.
        write(format!(r#"{{"version":1,"rooms":{{"{key}":0}}}}"#));
        assert!(!gate.allows_greeting("g", Some(ROOM)), "0 means never seen a human");
        write(format!(r#"{{"version":1,"rooms":{{"some-other-room":{now}}}}}"#));
        assert!(!gate.allows_greeting("g", Some(ROOM)), "a room the writer never saw");

        // Nothing to read, nothing to parse, nothing this bridge knows:
        // each refuses rather than guessing a greeting.
        std::fs::remove_file(&path).expect("remove the snapshot");
        assert!(!gate.allows_greeting("g", Some(ROOM)), "a missing snapshot");
        write("not json at all".to_owned());
        assert!(!gate.allows_greeting("g", Some(ROOM)), "an unparsable snapshot");
        write(format!(r#"{{"version":2,"rooms":{{"{key}":{now}}}}}"#));
        assert!(!gate.allows_greeting("g", Some(ROOM)), "an unknown snapshot version");

        // A group with no room name cannot be matched to any heartbeats.
        write(format!(r#"{{"version":1,"rooms":{{"{key}":{now}}}}}"#));
        assert!(!gate.allows_greeting("g", None), "no room name, no greeting");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn presence_snapshot_freshness_counts_exactly() {
        let json = r#"{"version":1,"rooms":{"k":100000}}"#;
        assert!(PresenceGate::fresh_in(json, "k", 144_999, 45_000).expect("fresh"));
        assert!(!PresenceGate::fresh_in(json, "k", 145_000, 45_000).expect("parsed"), "exactly ttl old is stale");
        assert!(!PresenceGate::fresh_in(json, "other", 100_001, 45_000).expect("parsed"), "a different room");
        assert!(
            !PresenceGate::fresh_in(r#"{"version":1,"rooms":{"k":0}}"#, "k", u64::MAX, 45_000).expect("parsed"),
            "0 is the writer's never-seen-a-human marker"
        );
        assert!(
            PresenceGate::fresh_in(r#"{"rooms":{"k":100000}}"#, "k", 100_001, 45_000).is_err(),
            "a snapshot with no version is refused, not assumed"
        );
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

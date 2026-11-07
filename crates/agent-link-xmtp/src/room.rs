//! `xmtp-bridge-room`: the bridge's room-side XMTP worker process.
//!
//! Owns a second XMTP identity whose only job is to be a member of other
//! people's groups: a web3 shoutbox room leader adds this identity's address
//! to each sliding-window group it creates, like any other participant. The
//! worker reads [`RoomRequest`]s (one JSON object per line) on stdin, writes
//! [`RoomEvent`]s on stdout and logs to stderr. It is started and restarted
//! by `xmtp-bridge`; it opens no listening socket.
//!
//! It relays every text message written by anyone else in any group it has
//! been added to; deciding what deserves the agent's attention (triggers,
//! per-person rates) is the bridge's job, not this process's. Only groups
//! are streamed: DMs to this identity are never read.

#[path = "common.rs"]
mod common;

use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::mpsc;

use anyhow::{Context, bail};
use bridge_core::room_protocol::{RoomEvent, RoomRequest};
use clap::Parser;
use xmtp::{Client, ConsentState, ConversationType, Message, MessageKind, Signer};

use crate::common::{Network, emit, open_client, short, text_of};

/// Room-side XMTP worker for xmtp-bridge (started by `xmtp-bridge run`).
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// XMTP network; must match the room's.
    #[arg(long, value_enum)]
    env: Network,
    /// Local XMTP database.
    #[arg(long)]
    db_path: PathBuf,
    /// File holding the room identity's Ethereum private key (hex, mode 600).
    #[arg(long)]
    key_file: PathBuf,
    /// File holding the 32-byte database encryption key (hex, mode 600).
    #[arg(long)]
    db_key_file: PathBuf,
}

enum Input {
    Request(RoomRequest),
    StdinClosed,
    Streamed { message_id: String },
    StreamEnded,
    /// A new group membership arrived: sync now instead of waiting for resync.
    NewConversation { group: String },
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let error = format!("{error:#}");
            log(&format!("fatal: {error}"));
            emit(&RoomEvent::Fatal { error });
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> anyhow::Result<()> {
    let app = concat!("xmtp-bridge-room/", env!("CARGO_PKG_VERSION"));
    let (client, signer) = open_client(args.env, &args.db_path, &args.key_file, &args.db_key_file, app)?;
    let inbox_id = client.inbox_id()?;
    let address = signer.identifier().address;

    // Groups the bridge was added to arrive as welcomes with Unknown consent.
    const CONSENT: &[ConsentState] = &[ConsentState::Unknown, ConsentState::Allowed];
    if let Err(error) = client.sync_all(CONSENT) {
        log(&format!("initial sync failed (continuing): {error}"));
    }

    let (tx, rx) = mpsc::channel::<Input>();
    let subscription = xmtp::stream::messages(&client, Some(ConversationType::Group), CONSENT)
        .context("starting the group message stream")?;
    let stream_tx = tx.clone();
    std::thread::Builder::new().name("xmtp-stream".into()).spawn(move || {
        for event in subscription {
            if stream_tx.send(Input::Streamed { message_id: event.message_id }).is_err() {
                return;
            }
        }
        let _ = stream_tx.send(Input::StreamEnded);
    })?;

    // Push-driven discovery: a welcome for a group we were just added to
    // triggers an immediate sync instead of waiting out the resync poll.
    // Failure to start or end is not fatal — the periodic resync below
    // remains as the backup path.
    match xmtp::stream::conversations(&client, Some(ConversationType::Group)) {
        Ok(conversations) => {
            let conv_tx = tx.clone();
            std::thread::Builder::new().name("xmtp-conversations".into()).spawn(move || {
                for conversation in conversations {
                    let group = conversation.id();
                    if conv_tx.send(Input::NewConversation { group }).is_err() {
                        return;
                    }
                }
                // Stream ended: bail via StreamEnded so the supervisor
                // restarts us with fresh streams instead of leaving
                // discovery silently dead.
                let _ = conv_tx.send(Input::StreamEnded);
            })?;
        }
        Err(error) => log(&format!("conversation stream unavailable (polling only): {error}")),
    }

    std::thread::Builder::new().name("stdin".into()).spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<RoomRequest>(&line) {
                Ok(request) => {
                    if tx.send(Input::Request(request)).is_err() {
                        return;
                    }
                }
                Err(error) => log(&format!("ignoring malformed request: {error}")),
            }
        }
        let _ = tx.send(Input::StdinClosed);
    })?;

    log(&format!("ready as {address} (inbox {})", short(&inbox_id)));
    emit(&RoomEvent::Ready { inbox_id: inbox_id.clone(), address });

    let mut worker = Worker {
        client,
        inbox_id,
        members: HashMap::new(),
        names: HashMap::new(),
        joined: std::collections::HashSet::new(),
        seen: std::collections::HashSet::new(),
    };
    // Groups surfacing in the first seconds are pre-existing memberships
    // replayed at startup, not fresh joins — seed them silently so a
    // bridge restart never re-greets old rooms.
    let started = std::time::Instant::now();
    const JOIN_ANNOUNCE_AFTER: std::time::Duration = std::time::Duration::from_secs(15);
    // Sliding-window rooms are recreated every few minutes by the room
    // leader, so re-sync periodically to discover groups created after
    // startup; without this the bridge only ever sees startup-time groups.
    // On XMTP rate limits the interval backs off exponentially instead of
    // re-hammering the API every 30s and keeping the quota exhausted.
    // Full syncs are expensive (every accumulated window group is
    // checked) and were the rate-limit sink: the conversations stream is
    // the primary discovery path now, so the full resync is a slow backup.
    const RESYNC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
    const RESYNC_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(30 * 60);
    let mut resync_interval = RESYNC_INTERVAL;
    // Cheap discovery tick, independent of the expensive full sync: pull
    // welcomes (one API call) and scan the local conversation list for
    // groups neither stream has surfaced. The full sync stays a slow
    // backup and may sit in rate-limit backoff for 30 minutes; discovery
    // of a fresh room must not wait for it.
    const DISCOVER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
    let mut last_full_sync = std::time::Instant::now();
    // One sync attempt with rate-limit backoff; shared by the poll timer
    // and push-driven discovery.
    let sync_now = |client: &Client, interval: &mut std::time::Duration| {
        match client.sync_all(CONSENT) {
            Ok(_) => *interval = RESYNC_INTERVAL,
            Err(error) => {
                let text = format!("{error:#}");
                if text.to_lowercase().contains("rate limit")
                    || text.contains("RESOURCE_EXHAUSTED")
                    || text.to_lowercase().contains("resource has been exhausted")
                {
                    *interval = (*interval * 2).min(RESYNC_BACKOFF_MAX);
                    log(&format!(
                        "periodic sync rate-limited, backing off to {}s",
                        interval.as_secs()
                    ));
                } else {
                    log(&format!("periodic sync failed (continuing): {error}"));
                }
            }
        }
    };
    loop {
        match rx.recv_timeout(DISCOVER_INTERVAL) {
            Ok(Input::NewConversation { group }) => {
                // Cheap targeted welcome sync (not sync_all): process the
                // new invitation without re-checking every dead window group.
                if let Err(error) = worker.client.sync_welcomes() {
                    log(&format!("welcome sync failed (continuing): {error}"));
                }
                if worker.joined.insert(group.clone()) && started.elapsed() > JOIN_ANNOUNCE_AFTER {
                    let group_name = worker.group_name(&group);
                    emit(&RoomEvent::Joined { group, group_name });
                }
            }
            Ok(Input::Request(request)) => worker.handle(request),
            Ok(Input::Streamed { message_id }) => match worker.client.message_by_id(&message_id) {
                Ok(Some(message)) => {
                    // A message in a group discovery never announced is
                    // still a join: announce it or the room gets no greeting.
                    if worker.joined.insert(message.conversation_id.clone())
                        && started.elapsed() > JOIN_ANNOUNCE_AFTER
                    {
                        let group = message.conversation_id.clone();
                        let group_name = worker.group_name(&group);
                        emit(&RoomEvent::Joined { group, group_name });
                    }
                    worker.relay(&message);
                }
                Ok(None) => {}
                Err(error) => log(&format!("loading message {}: {error}", short(&message_id))),
            },
            // The bridge closed our stdin: orderly shutdown.
            Ok(Input::StdinClosed) => return Ok(()),
            // The supervisor restarts us.
            Ok(Input::StreamEnded) => bail!("XMTP message stream ended"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                worker.discover(started.elapsed() > JOIN_ANNOUNCE_AFTER);
                if last_full_sync.elapsed() >= resync_interval {
                    sync_now(&worker.client, &mut resync_interval);
                    last_full_sync = std::time::Instant::now();
                }
                worker.backfill(started.elapsed() > JOIN_ANNOUNCE_AFTER);
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

/// Most groups whose member lists are cached (a shoutbox makes a new group
/// every few minutes; old ones go quiet).
const MAX_CACHED_GROUPS: usize = 256;

struct Worker {
    client: Client,
    inbox_id: String,
    /// Per group: member inbox id → Ethereum address.
    members: HashMap<String, HashMap<String, String>>,
    /// Per group: its XMTP name, once read. The shoutbox stamps the room URL
    /// into the name when the leader creates a group, so this is how the
    /// bridge learns which room a group is. Names are cached because every
    /// relayed line would otherwise re-read group metadata; a group with no
    /// name yet is *not* cached, so a metadata update that arrives later is
    /// picked up on the next message rather than being remembered as absent.
    names: HashMap<String, String>,
    /// Groups already announced via `RoomEvent::Joined` (or seeded at startup).
    joined: std::collections::HashSet<String>,
    /// Message ids already relayed (stream + backfill dedup).
    seen: std::collections::HashSet<String>,
}

impl Worker {
    fn handle(&self, request: RoomRequest) {
        match request {
            RoomRequest::Send { req, group, text } => {
                // Name every post before the blocking XMTP call so a wedge
                // is a visible stuck operation, not silence.
                log(&format!("posting req {req} to group {} ({} chars)", short(&group), text.len()));
                match self.send(&group, &text) {
                    Ok(()) => {
                        log(&format!("posted req {req} to group {}", short(&group)));
                        emit(&RoomEvent::SendOk { req })
                    }
                    Err(error) => {
                        log(&format!("post req {req} to group {} failed: {error:#}", short(&group)));
                        emit(&RoomEvent::SendFailed { req, error: format!("{error:#}") })
                    }
                }
            }
        }
    }

    fn send(&self, group: &str, text: &str) -> anyhow::Result<()> {
        let conversation =
            self.client.conversation(group)?.with_context(|| format!("unknown group {}", short(group)))?;
        conversation.send_text(text)?;
        Ok(())
    }

    /// Discover groups neither stream surfaced: sync welcomes (cheap),
    /// then scan the local conversation list. New groups are announced
    /// exactly like stream-discovered ones.
    fn discover(&mut self, announce: bool) {
        if let Err(error) = self.client.sync_welcomes() {
            log(&format!("discovery welcome-sync failed (continuing): {error:#}"));
            return;
        }
        let Ok(conversations) = self.client.conversations() else { return };
        for conversation in conversations {
            let group = conversation.id();
            if is_new_group(&self.joined, &self.members, &group) {
                self.joined.insert(group.clone());
                if announce {
                    log(&format!("discovered group {} via welcome sync", short(&group)));
                    let group_name = self.group_name(&group);
                    emit(&RoomEvent::Joined { group, group_name });
                }
            }
        }
    }

    /// Relay recent history the live stream missed (post-restart or
    /// undiscovered groups). Deduped via `seen`, shared with the stream.
    fn backfill(&mut self, announce: bool) {
        // Announced groups (joined) plus any group with cached members.
        let mut groups: Vec<String> = self.joined.iter().cloned().collect();
        for group in self.members.keys() {
            if !self.joined.contains(group) {
                groups.push(group.clone());
            }
        }
        for group in groups {
            // Backfill reaching a group discovery missed is a join too.
            if self.joined.insert(group.clone()) && announce {
                let group_name = self.group_name(&group);
                emit(&RoomEvent::Joined { group: group.clone(), group_name });
            }
            let Ok(Some(conversation)) = self.client.conversation(&group) else { continue };
            let Ok(messages) = conversation.messages() else { continue };
            let recent: Vec<&Message> = messages.iter().rev().take(25).collect();
            for message in recent.into_iter().rev() {
                self.relay(message);
            }
        }
    }

    /// Forward one message written by someone else; everything else is ignored.
    fn relay(&mut self, message: &Message) {
        if !self.seen.insert(message.id.clone()) {
            return; // already relayed (stream/backfill overlap)
        }
        if self.seen.len() > 5_000 {
            self.seen.clear();
            self.seen.insert(message.id.clone());
        }
        if message.sender_inbox_id == self.inbox_id || message.kind != MessageKind::Application {
            return;
        }
        let Some(text) = text_of(message, "[sent an attachment]") else { return };
        let sender_address = self.address_of(&message.conversation_id, &message.sender_inbox_id);
        let group_name = self.group_name(&message.conversation_id);
        emit(&RoomEvent::Message {
            group: message.conversation_id.clone(),
            id: message.id.clone(),
            sender_inbox_id: message.sender_inbox_id.clone(),
            sender_address,
            sent_at_ns: message.sent_at_ns,
            text,
            group_name,
        });
    }

    /// The group's XMTP name: the room URL the shoutbox's leader stamped
    /// into the group when it created it, and the only thing the bridge can
    /// match a group against to tell which room it is.
    ///
    /// Found names are cached, since every relayed line would otherwise
    /// re-read group metadata. A group with no name yet is *not* cached, so
    /// metadata that arrives after the group does is picked up on the next
    /// message instead of being remembered as absent forever.
    fn group_name(&mut self, group: &str) -> Option<String> {
        if let Some(name) = self.names.get(group) {
            return Some(name.clone());
        }
        let name = match self.client.conversation(group) {
            Ok(Some(conversation)) => conversation.name(),
            Ok(None) => return None,
            Err(error) => {
                log(&format!("reading the name of {}: {error}", short(group)));
                return None;
            }
        }?;
        if self.names.len() >= MAX_CACHED_GROUPS && !self.names.contains_key(group) {
            self.names.clear();
        }
        self.names.insert(group.to_owned(), name.clone());
        Some(name)
    }

    /// The sender's address from the group's member list, refreshing the
    /// cached list once on a miss (new members join mid-window).
    fn address_of(&mut self, group: &str, inbox_id: &str) -> Option<String> {
        if let Some(address) = self.members.get(group).and_then(|m| m.get(inbox_id)) {
            return Some(address.clone());
        }
        let refreshed = match self.client.conversation(group) {
            Ok(Some(conversation)) => match conversation.members() {
                Ok(members) => members
                    .into_iter()
                    .filter_map(|m| m.account_identifiers.into_iter().next().map(|a| (m.inbox_id, a)))
                    .collect::<HashMap<_, _>>(),
                Err(error) => {
                    log(&format!("listing members of {}: {error}", short(group)));
                    return None;
                }
            },
            Ok(None) => return None,
            Err(error) => {
                log(&format!("loading group {}: {error}", short(group)));
                return None;
            }
        };
        if self.members.len() >= MAX_CACHED_GROUPS && !self.members.contains_key(group) {
            self.members.clear();
        }
        let address = refreshed.get(inbox_id).cloned();
        self.members.insert(group.to_owned(), refreshed);
        address
    }
}

fn log(text: &str) {
    eprintln!("xmtp-bridge-room: {text}");
}

/// A group is new when neither the joined set nor the member cache has
/// seen it; both are populated by stream discovery and by `discover`.
fn is_new_group(
    joined: &std::collections::HashSet<String>,
    members: &HashMap<String, HashMap<String, String>>,
    group: &str,
) -> bool {
    !joined.contains(group) && !members.contains_key(group)
}

#[cfg(test)]
mod tests {
    use super::is_new_group;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn welcome_discovery_only_announces_unseen_groups() {
        let joined: HashSet<String> = ["g-stream".to_string()].into_iter().collect();
        let mut members: HashMap<String, HashMap<String, String>> = HashMap::new();
        members.insert("g-cached".to_string(), HashMap::new());
        assert!(!is_new_group(&joined, &members, "g-stream"));
        assert!(!is_new_group(&joined, &members, "g-cached"));
        assert!(is_new_group(&joined, &members, "g-fresh"));
    }
}

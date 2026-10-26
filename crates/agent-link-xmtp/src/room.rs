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

    let mut worker = Worker { client, inbox_id, members: HashMap::new(), joined: std::collections::HashSet::new() };
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
    const RESYNC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
    const RESYNC_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(30 * 60);
    let mut resync_interval = RESYNC_INTERVAL;
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
        match rx.recv_timeout(resync_interval) {
            Ok(Input::NewConversation { group }) => {
                sync_now(&worker.client, &mut resync_interval);
                if worker.joined.insert(group.clone()) && started.elapsed() > JOIN_ANNOUNCE_AFTER {
                    emit(&RoomEvent::Joined { group });
                }
            }
            Ok(Input::Request(request)) => worker.handle(request),
            Ok(Input::Streamed { message_id }) => match worker.client.message_by_id(&message_id) {
                Ok(Some(message)) => worker.relay(&message),
                Ok(None) => {}
                Err(error) => log(&format!("loading message {}: {error}", short(&message_id))),
            },
            // The bridge closed our stdin: orderly shutdown.
            Ok(Input::StdinClosed) => return Ok(()),
            // The supervisor restarts us.
            Ok(Input::StreamEnded) => bail!("XMTP message stream ended"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                sync_now(&worker.client, &mut resync_interval)
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
    /// Groups already announced via `RoomEvent::Joined` (or seeded at startup).
    joined: std::collections::HashSet<String>,
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

    /// Forward one message written by someone else; everything else is ignored.
    fn relay(&mut self, message: &Message) {
        if message.sender_inbox_id == self.inbox_id || message.kind != MessageKind::Application {
            return;
        }
        let Some(text) = text_of(message, "[sent an attachment]") else { return };
        let sender_address = self.address_of(&message.conversation_id, &message.sender_inbox_id);
        emit(&RoomEvent::Message {
            group: message.conversation_id.clone(),
            id: message.id.clone(),
            sender_inbox_id: message.sender_inbox_id.clone(),
            sender_address,
            sent_at_ns: message.sent_at_ns,
            text,
        });
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

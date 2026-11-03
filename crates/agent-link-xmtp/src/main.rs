//! `xmtp-bridge-agent`: the bridge's XMTP worker process.
//!
//! Owns the bridge's XMTP identity and client. Reads
//! [`AgentRequest`]s (one JSON object per line) on stdin, writes
//! [`AgentEvent`]s on stdout, logs to stderr. It is started and restarted by
//! `xmtp-bridge`; it opens no listening socket.
//!
//! Each front-end user maps to one XMTP group containing only the bridge and
//! the agent, created with admin-only permissions so the agent cannot add
//! anyone else. Only messages written by the agent's inbox in those groups
//! are relayed back.

#[path = "common.rs"]
mod common;

use std::io::BufRead;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::mpsc;

use anyhow::{Context, bail};
use bridge_core::protocol::{AgentEvent, AgentRequest, ResumeCursor};
use clap::Parser;
use xmtp::{
    Client, ConsentState, CreateGroupOptions, GroupPermissionsPreset, IdentifierKind, ListMessagesOptions, Message,
    MessageKind, Recipient, Signer, SortDirection,
};

use crate::common::{Network, emit, open_client, short, text_of};

/// XMTP worker for xmtp-bridge (started by `xmtp-bridge run`).
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// XMTP network; must match the agent's.
    #[arg(long, value_enum)]
    env: Network,
    /// The agent: an Ethereum address (0x…) or an XMTP inbox id.
    #[arg(long)]
    agent: String,
    /// Local XMTP database.
    #[arg(long)]
    db_path: PathBuf,
    /// File holding the bridge's Ethereum private key (hex, mode 600).
    #[arg(long)]
    key_file: PathBuf,
    /// File holding the 32-byte database encryption key (hex, mode 600).
    #[arg(long)]
    db_key_file: PathBuf,
}

enum Input {
    Request(AgentRequest),
    StdinClosed,
    Streamed { message_id: String },
    StreamEnded,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let error = format!("{error:#}");
            log(&format!("fatal: {error}"));
            emit(&AgentEvent::Fatal { error });
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> anyhow::Result<()> {
    let app = concat!("xmtp-bridge/", env!("CARGO_PKG_VERSION"));
    let (client, signer) = open_client(args.env, &args.db_path, &args.key_file, &args.db_key_file, app)?;
    let inbox_id = client.inbox_id()?;
    let address = signer.identifier().address;
    let agent_inbox_id = resolve_agent(&client, &args.agent)?;
    anyhow::ensure!(agent_inbox_id != inbox_id, "the agent and the bridge are the same XMTP identity");

    if let Err(error) = client.sync_all(&[ConsentState::Allowed]) {
        log(&format!("initial sync failed (continuing): {error}"));
    }

    let (tx, rx) = mpsc::channel::<Input>();

    // Only conversations the bridge allowed (the groups it created) are streamed;
    // anyone else writing to the bridge's address is never read.
    let subscription =
        xmtp::stream::messages(&client, None, &[ConsentState::Allowed]).context("starting the message stream")?;
    let stream_tx = tx.clone();
    std::thread::Builder::new().name("xmtp-stream".into()).spawn(move || {
        for event in subscription {
            if stream_tx.send(Input::Streamed { message_id: event.message_id }).is_err() {
                return;
            }
        }
        let _ = stream_tx.send(Input::StreamEnded);
    })?;

    std::thread::Builder::new().name("stdin".into()).spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<AgentRequest>(&line) {
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

    log(&format!("ready as {address} (inbox {}), agent inbox {}", short(&inbox_id), short(&agent_inbox_id)));
    emit(&AgentEvent::Ready { inbox_id, address, agent_inbox_id: agent_inbox_id.clone() });

    let mut worker = Worker {
        client,
        agent_inbox_id,
        threads: std::collections::HashSet::new(),
        seen: std::collections::HashSet::new(),
    };
    loop {
        let input = match rx.recv_timeout(BACKFILL_INTERVAL) {
            Ok(input) => input,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                worker.backfill();
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        };
        match input {
            Input::Request(request) => worker.handle(request),
            Input::Streamed { message_id } => match worker.client.message_by_id(&message_id) {
                Ok(Some(message)) => {
                    log(&format!(
                        "agent event {} in {}",
                        short(&message.id),
                        short(&message.conversation_id)
                    ));
                    worker.threads.insert(message.conversation_id.clone());
                    worker.relay(&message);
                }
                Ok(None) => {
                    log(&format!(
                        "agent event {} not found after stream delivery",
                        short(&message_id)
                    ));
                }
                Err(error) => log(&format!("loading message {}: {error}", short(&message_id))),
            },
            // The bridge closed our stdin: orderly shutdown.
            Input::StdinClosed => return Ok(()),
            // The supervisor restarts us and replays from the cursors.
            Input::StreamEnded => bail!("XMTP message stream ended"),
        }
    }
    Ok(())
}

fn resolve_agent(client: &Client, agent: &str) -> anyhow::Result<String> {
    match Recipient::parse(agent) {
        Recipient::Address(address) => client
            .inbox_id_for(&address, IdentifierKind::Ethereum)?
            .with_context(|| format!("agent address {address} has no XMTP inbox on this network")),
        Recipient::InboxId(inbox_id) => Ok(inbox_id),
        Recipient::Ens(name) => {
            bail!("ENS names are not supported for the agent ({name}); use its address or inbox id")
        }
    }
}

/// How often known agent threads are re-synced and backfilled. The live
/// stream can hang open while delivering nothing (a zombie stream never
/// ends, so the supervisor's stream-end restart never fires); this makes
/// reply loss a delay of at most one interval instead of silence.
const BACKFILL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(120);

struct Worker {
    client: Client,
    agent_inbox_id: String,
    /// Agent threads opened, resumed, or seen on the stream.
    threads: std::collections::HashSet<String>,
    /// Message ids already relayed (stream + backfill dedup).
    seen: std::collections::HashSet<String>,
}

/// True the first time an id is marked seen; false for repeats.
fn first_seen(seen: &mut std::collections::HashSet<String>, id: &str) -> bool {
    if !seen.insert(id.to_string()) {
        return false;
    }
    if seen.len() > 5_000 {
        seen.clear();
        seen.insert(id.to_string());
    }
    true
}

impl Worker {
    fn handle(&mut self, request: AgentRequest) {
        match request {
            AgentRequest::OpenThread { req } => {
                // Opening while rate-limited wedges silently in the FFI:
                // sync welcomes first (cheap), log an outcome for every open.
                if let Err(error) = self.client.sync_welcomes() {
                    log(&format!(
                        "pre-open welcome-sync failed for req {req} (continuing): {error:#}"
                    ));
                }
                let options =
                    CreateGroupOptions { permissions: Some(GroupPermissionsPreset::AdminOnly), ..Default::default() };
                match self
                    .client
                    .group(
                        &[Recipient::InboxId(self.agent_inbox_id.clone())],
                        &options,
                    ) {
                    Ok(conversation) => {
                        self.threads.insert(conversation.id());
                        log(&format!(
                            "agent thread opened: {}",
                            short(&conversation.id())
                        ));
                        emit(&AgentEvent::Opened {
                            req,
                            thread: conversation.id(),
                        });
                    }
                    Err(error) => {
                        log(&format!("agent thread open failed: {error:#}"));
                        emit(&AgentEvent::OpenFailed {
                            req,
                            error: error.to_string(),
                        });
                    }
                }
            }
            AgentRequest::Send { req, thread, text } => {
                log(&format!("agent delivery send req {req}"));
                match self.send(&thread, &text) {
                    Ok(()) => {
                        log(&format!("agent delivery sent req {req}"));
                        emit(&AgentEvent::SendOk { req })
                    }
                    Err(error) => {
                        log(&format!("agent delivery failed: {error:#}"));
                        emit(&AgentEvent::SendFailed {
                            req,
                            error: format!("{error:#}"),
                        })
                    }
                }
            }
            AgentRequest::Resume { threads } => {
                for cursor in threads {
                    self.threads.insert(cursor.thread.clone());
                    if let Err(error) = self.resume(&cursor) {
                        log(&format!("resume {}: {error:#}", short(&cursor.thread)));
                    }
                }
            }
        }
    }

    fn send(&self, thread: &str, text: &str) -> anyhow::Result<()> {
        let conversation =
            self.client.conversation(thread)?.with_context(|| format!("unknown thread {}", short(thread)))?;
        conversation.send_text(text)?;
        Ok(())
    }

    /// Relay recent agent replies the live stream missed. Runs on the
    /// recv-timeout tick; per-thread sync pulls what a zombie stream never
    /// delivered, and `seen` dedups against stream deliveries.
    fn backfill(&mut self) {
        if let Err(error) = self.client.sync_welcomes() {
            log(&format!("backfill welcome-sync failed (continuing): {error:#}"));
        }
        let threads: Vec<String> = self.threads.iter().cloned().collect();
        for thread in threads {
            let Ok(Some(conversation)) = self.client.conversation(&thread) else { continue };
            if let Err(error) = conversation.sync() {
                log(&format!("backfill sync {} failed (continuing): {error:#}", short(&thread)));
                continue;
            }
            let Ok(messages) = conversation.messages() else { continue };
            let recent: Vec<&Message> = messages.iter().rev().take(25).collect();
            for message in recent.into_iter().rev() {
                self.relay(message);
            }
        }
    }

    fn resume(&mut self, cursor: &ResumeCursor) -> anyhow::Result<()> {
        let Some(conversation) = self.client.conversation(&cursor.thread)? else {
            bail!("unknown thread");
        };
        conversation.sync()?;
        let options = ListMessagesOptions {
            sent_after_ns: cursor.after_ns,
            direction: Some(SortDirection::Ascending),
            kind: Some(MessageKind::Application),
            ..Default::default()
        };
        for message in conversation.list_messages(&options)? {
            self.relay(&message);
        }
        Ok(())
    }

    /// Forward one agent message to the bridge; everything else is ignored.
    fn relay(&mut self, message: &Message) {
        if !first_seen(&mut self.seen, &message.id) {
            return; // already relayed (stream/backfill overlap)
        }
        if message.sender_inbox_id != self.agent_inbox_id || message.kind != MessageKind::Application {
            return;
        }
        let Some(text) = text_of(message, "[The agent sent an attachment. This bridge only relays text.]") else {
            return;
        };
        emit(&AgentEvent::Message {
            thread: message.conversation_id.clone(),
            id: message.id.clone(),
            sent_at_ns: message.sent_at_ns,
            text,
        });
    }
}

fn log(text: &str) {
    eprintln!("xmtp-bridge-agent: {text}");
}

#[cfg(test)]
mod tests {
    use super::first_seen;
    use std::collections::HashSet;

    #[test]
    fn backfill_dedup_relays_a_missed_reply_once() {
        let mut seen = HashSet::new();
        // Stream delivered this one; backfill must not relay it again.
        assert!(first_seen(&mut seen, "streamed-1"));
        assert!(!first_seen(&mut seen, "streamed-1"));
        // Stream missed this reply; backfill relays it exactly once.
        assert!(first_seen(&mut seen, "missed-1"));
        assert!(!first_seen(&mut seen, "missed-1"));
    }
}

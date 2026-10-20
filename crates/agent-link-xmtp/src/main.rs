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

    let worker = Worker { client, agent_inbox_id };
    for input in rx {
        match input {
            Input::Request(request) => worker.handle(request),
            Input::Streamed { message_id } => match worker.client.message_by_id(&message_id) {
                Ok(Some(message)) => worker.relay(&message),
                Ok(None) => {}
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

struct Worker {
    client: Client,
    agent_inbox_id: String,
}

impl Worker {
    fn handle(&self, request: AgentRequest) {
        match request {
            AgentRequest::OpenThread { req } => {
                let options =
                    CreateGroupOptions { permissions: Some(GroupPermissionsPreset::AdminOnly), ..Default::default() };
                match self.client.group(&[Recipient::InboxId(self.agent_inbox_id.clone())], &options) {
                    Ok(conversation) => emit(&AgentEvent::Opened { req, thread: conversation.id() }),
                    Err(error) => emit(&AgentEvent::OpenFailed { req, error: error.to_string() }),
                }
            }
            AgentRequest::Send { req, thread, text } => match self.send(&thread, &text) {
                Ok(()) => emit(&AgentEvent::SendOk { req }),
                Err(error) => emit(&AgentEvent::SendFailed { req, error: format!("{error:#}") }),
            },
            AgentRequest::Resume { threads } => {
                for cursor in threads {
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

    fn resume(&self, cursor: &ResumeCursor) -> anyhow::Result<()> {
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
    fn relay(&self, message: &Message) {
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

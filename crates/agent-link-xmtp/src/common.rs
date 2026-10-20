//! Helpers shared by the two XMTP worker binaries (included with `#[path]`).

use std::io::Write;
use std::path::Path;

use anyhow::Context;
use bridge_core::secrets::{read_hex_secret, read_secret_file};
use clap::ValueEnum;
use serde::Serialize;
use xmtp::content::Content;
use xmtp::{AlloySigner, Client, Env, Message};

/// XMTP network, as a CLI flag.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Network {
    Production,
    Dev,
    Local,
}

impl From<Network> for Env {
    fn from(network: Network) -> Self {
        match network {
            Network::Production => Self::Production,
            Network::Dev => Self::Dev,
            Network::Local => Self::Local,
        }
    }
}

/// Open the local XMTP database and client for the identity in `key_file`.
pub fn open_client(
    env: Network,
    db_path: &Path,
    key_file: &Path,
    db_key_file: &Path,
    app: &str,
) -> anyhow::Result<(Client, AlloySigner)> {
    let signer = AlloySigner::from_hex(&read_secret_file(key_file)?).context("parsing the Ethereum key")?;
    let db_key = read_hex_secret(db_key_file, 32)?;
    if let Some(dir) = db_path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let client = Client::builder()
        .env(env.into())
        .db_path(db_path.to_string_lossy())
        .encryption_key(db_key)?
        .app_version(app)
        .build(&signer)
        .context("creating the XMTP client")?;
    Ok((client, signer))
}

/// The user-visible text of a message, if it has any.
pub fn text_of(message: &Message, attachment_notice: &str) -> Option<String> {
    match message.decode() {
        Ok(Content::Text(text) | Content::Markdown(text)) => Some(text),
        Ok(Content::Attachment(_) | Content::RemoteAttachment(_) | Content::MultiRemoteAttachment(_)) => {
            Some(attachment_notice.to_owned())
        }
        // Receipts, reactions and control messages carry nothing to relay.
        Ok(
            Content::ReadReceipt | Content::Reaction(_) | Content::DeleteMessage { .. } | Content::LeaveRequest { .. },
        ) => None,
        Ok(_) | Err(_) => message.fallback.clone().filter(|f| !f.trim().is_empty()),
    }
}

/// Write one protocol line to stdout.
pub fn emit<T: Serialize>(event: &T) {
    let Ok(line) = serde_json::to_string(event) else { return };
    let mut stdout = std::io::stdout().lock();
    // A closed stdout means the bridge is gone; stdin closing ends the loop.
    let _ = writeln!(stdout, "{line}").and_then(|()| stdout.flush());
}

/// First 8 characters of an id, for logs.
pub fn short(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

//! JSON-lines protocol between the bridge and its XMTP worker process.
//!
//! The XMTP SDK runs in a separate child process (`xmtp-bridge-agent`) that
//! reads one [`AgentRequest`] per line on stdin and writes one [`AgentEvent`]
//! per line on stdout; its logs go to stderr. Keeping it in its own process
//! means libxmtp's statically linked runtime (its own Rust std, SQLCipher)
//! never shares an address space with presage's, and a crash in either SDK
//! cannot take the other down. Nothing listens on a port.
//!
//! The worker never learns who a front-end user is: it only sees opaque
//! request numbers and XMTP thread ids.

use serde::{Deserialize, Serialize};

use crate::worker::WorkerEvent;

/// Where to resume delivering agent messages for one thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeCursor {
    /// XMTP conversation id.
    pub thread: String,
    /// Deliver agent messages sent strictly after this time (ns).
    pub after_ns: i64,
}

/// Bridge → worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum AgentRequest {
    /// Create a new XMTP group with the agent; answered by
    /// [`AgentEvent::Opened`] or [`AgentEvent::OpenFailed`].
    OpenThread {
        /// Correlation id.
        req: u64,
    },
    /// Send text into a thread; answered by [`AgentEvent::SendOk`] or
    /// [`AgentEvent::SendFailed`].
    Send {
        /// Correlation id.
        req: u64,
        /// XMTP conversation id.
        thread: String,
        /// Plain text.
        text: String,
    },
    /// Re-emit agent messages newer than each cursor (after a restart).
    Resume {
        /// One cursor per known thread.
        threads: Vec<ResumeCursor>,
    },
}

/// Worker → bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ev", rename_all = "snake_case")]
pub enum AgentEvent {
    /// Connected and streaming; requests are accepted from now on.
    Ready {
        /// The bridge's own XMTP inbox id.
        inbox_id: String,
        /// The bridge's Ethereum address.
        address: String,
        /// The agent's inbox id.
        agent_inbox_id: String,
    },
    /// A thread was created.
    Opened {
        /// Correlation id from [`AgentRequest::OpenThread`].
        req: u64,
        /// XMTP conversation id.
        thread: String,
    },
    /// A thread could not be created.
    OpenFailed {
        /// Correlation id.
        req: u64,
        /// Reason, for logs.
        error: String,
    },
    /// A send completed.
    SendOk {
        /// Correlation id from [`AgentRequest::Send`].
        req: u64,
    },
    /// A send failed.
    SendFailed {
        /// Correlation id.
        req: u64,
        /// Reason, for logs.
        error: String,
    },
    /// The agent wrote in a thread.
    Message {
        /// XMTP conversation id.
        thread: String,
        /// XMTP message id.
        id: String,
        /// Send time (ns).
        sent_at_ns: i64,
        /// Plain text (non-text content is replaced by a short notice).
        text: String,
    },
    /// The worker is about to exit.
    Fatal {
        /// Reason, for logs.
        error: String,
    },
}

impl WorkerEvent for AgentEvent {
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

/// Encode one protocol line (no trailing newline).
///
/// # Errors
/// Only if serialization fails, which these types cannot trigger.
pub fn encode<T: Serialize>(value: &T) -> serde_json::Result<String> {
    serde_json::to_string(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_format_is_stable() {
        let line = encode(&AgentRequest::Send { req: 7, thread: "ab".into(), text: "hi".into() }).unwrap();
        assert_eq!(line, r#"{"op":"send","req":7,"thread":"ab","text":"hi"}"#);
        let event: AgentEvent = serde_json::from_str(r#"{"ev":"opened","req":7,"thread":"ab"}"#).unwrap();
        assert_eq!(event, AgentEvent::Opened { req: 7, thread: "ab".into() });
    }

    #[test]
    fn round_trips_every_variant() {
        let requests = [
            AgentRequest::OpenThread { req: 1 },
            AgentRequest::Send { req: 2, thread: "t".into(), text: "line\nbreak \"quoted\"".into() },
            AgentRequest::Resume { threads: vec![ResumeCursor { thread: "t".into(), after_ns: 5 }] },
        ];
        for r in requests {
            let line = encode(&r).unwrap();
            assert!(!line.contains('\n'), "one request per line");
            assert_eq!(serde_json::from_str::<AgentRequest>(&line).unwrap(), r);
        }
    }
}

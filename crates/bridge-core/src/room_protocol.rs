//! JSON-lines protocol between the bridge and its room worker process.
//!
//! The room worker (`xmtp-bridge-room`) holds a second XMTP identity that
//! other people add to their groups — today, the web3 shoutbox's sliding
//! window rooms. It reports what group members write and posts the bridge's
//! replies back. It is a front-end-side worker: it never sees the agent, the
//! agent-link identity, or any rate limit.
//!
//! Real-time semantics, unlike [`crate::protocol`]: there are no delivery
//! cursors and nothing is replayed after a restart. A room is a live chat;
//! what was said while the bridge was away is not worth answering late.

use serde::{Deserialize, Serialize};

use crate::worker::WorkerEvent;

/// Bridge → room worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RoomRequest {
    /// Post text into a group; answered by [`RoomEvent::SendOk`] or
    /// [`RoomEvent::SendFailed`].
    Send {
        /// Correlation id.
        req: u64,
        /// XMTP group id.
        group: String,
        /// Plain text.
        text: String,
    },
}

/// Room worker → bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ev", rename_all = "snake_case")]
pub enum RoomEvent {
    /// Connected and streaming; requests are accepted from now on.
    Ready {
        /// The room identity's XMTP inbox id.
        inbox_id: String,
        /// The room identity's Ethereum address (what room leaders add).
        address: String,
    },
    /// Someone else wrote in a group the bridge is a member of.
    Message {
        /// XMTP group id.
        group: String,
        /// XMTP message id.
        id: String,
        /// The sender's XMTP inbox id (stable across groups).
        sender_inbox_id: String,
        /// The sender's Ethereum address, when the group's member list has it.
        sender_address: Option<String>,
        /// Send time (ns).
        sent_at_ns: i64,
        /// Plain text (non-text content is replaced by a short notice).
        text: String,
        /// The group's XMTP name. The shoutbox sets it to the room URL when
        /// the leader creates a group, and it is the only thing on this wire
        /// that says which room the group belongs to — the room key never
        /// crosses into the bridge. `None` when the group is unnamed or its
        /// metadata has not synced yet.
        #[serde(default)]
        group_name: Option<String>,
    },
    /// The worker was added to a new group (stream-discovered, not from
    /// the startup resync): the bridge may greet it.
    Joined {
        /// XMTP group id.
        group: String,
        /// The group's XMTP name, as in [`RoomEvent::Message::group_name`].
        /// The bridge refuses to greet a room outside its allowlist.
        #[serde(default)]
        group_name: Option<String>,
    },
    /// A send completed.
    SendOk {
        /// Correlation id from [`RoomRequest::Send`].
        req: u64,
    },
    /// A send failed.
    SendFailed {
        /// Correlation id.
        req: u64,
        /// Reason, for logs.
        error: String,
    },
    /// The worker is about to exit.
    Fatal {
        /// Reason, for logs.
        error: String,
    },
}

impl WorkerEvent for RoomEvent {
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_format_is_stable() {
        let line = serde_json::to_string(&RoomRequest::Send { req: 3, group: "g".into(), text: "hi".into() }).unwrap();
        assert_eq!(line, r#"{"op":"send","req":3,"group":"g","text":"hi"}"#);
        let event: RoomEvent = serde_json::from_str(
            r#"{"ev":"message","group":"g","id":"m","sender_inbox_id":"s","sender_address":null,"sent_at_ns":5,"text":"yo","group_name":"https://shoutbox.pinme.dev/default"}"#,
        )
        .unwrap();
        assert_eq!(
            event,
            RoomEvent::Message {
                group: "g".into(),
                id: "m".into(),
                sender_inbox_id: "s".into(),
                sender_address: None,
                sent_at_ns: 5,
                text: "yo".into(),
                group_name: Some("https://shoutbox.pinme.dev/default".into()),
            }
        );
        // A worker that predates the room name still deserializes: the field
        // defaults to `None`, which the bridge then treats as not allowed.
        let unnamed: RoomEvent = serde_json::from_str(
            r#"{"ev":"message","group":"g","id":"m","sender_inbox_id":"s","sender_address":null,"sent_at_ns":5,"text":"yo"}"#,
        )
        .unwrap();
        assert!(
            matches!(unnamed, RoomEvent::Message { group_name: None, .. }),
            "a missing group_name must not be a protocol error"
        );
        assert!(
            serde_json::from_str::<RoomEvent>(r#"{"ev":"joined","group":"g"}"#)
                .is_ok_and(|joined| matches!(joined, RoomEvent::Joined { group_name: None, .. })),
            "a missing group_name on Joined must not be a protocol error"
        );
        assert!(RoomEvent::Ready { inbox_id: "i".into(), address: "0x1".into() }.is_ready());
        assert!(!event.is_ready());
    }
}

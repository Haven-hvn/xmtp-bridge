//! Network-agnostic core of `xmtp-bridge`.
//!
//! The bridge sits between people on messaging networks ("front ends", e.g.
//! Signal) and one XMTP agent. Every front-end user gets their own XMTP group
//! with the agent, so the agent keeps a separate session per user.
//!
//! This crate holds everything that does not touch a network:
//!
//! - [`message`]: the normalized message model and the [`message::Frontend`]
//!   trait new integrations implement;
//! - [`bucket`]: token buckets (single and keyed);
//! - [`limiter`]: inbound admission (per sender, global, new senders);
//! - [`pacer`]: outbound pacing (per recipient + global) with bounded queues;
//! - [`sessions`]: the persisted user ↔ XMTP-thread map with LRU eviction;
//! - [`protocol`]: the JSON-lines protocol spoken with the agent-side XMTP
//!   worker, and [`room_protocol`], the one spoken with the room worker that
//!   joins other people's XMTP groups (the shoutbox front end);
//! - [`worker`]: the child-process supervisor both workers run under;
//! - [`router`]: the pure state machine tying it together, and [`runtime`],
//!   its async driver.

pub mod bucket;
pub mod dedupe;
pub mod limiter;
pub mod message;
pub mod pacer;
pub mod protocol;
pub mod room_protocol;
pub mod router;
pub mod runtime;
pub mod secrets;
pub mod sessions;
pub mod text;
pub mod worker;

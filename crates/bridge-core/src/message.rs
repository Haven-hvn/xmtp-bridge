//! The normalized message model and the front-end integration contract.
//!
//! A front end (Signal today; Matrix, Telegram, … later) translates its
//! network's messages into [`Inbound`] and delivers [`Outbound`] back. It never
//! sees XMTP, rate limits, or sessions.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

/// One user on one front-end network.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UserAddress {
    /// Network id, e.g. `"signal"`. Must match [`Frontend::network`].
    pub network: String,
    /// The network's stable id for the user (for Signal, the service id).
    pub user: String,
}

impl UserAddress {
    /// Construct an address.
    #[must_use]
    pub fn new(network: impl Into<String>, user: impl Into<String>) -> Self {
        Self { network: network.into(), user: user.into() }
    }
}

impl fmt::Display for UserAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.network, self.user)
    }
}

/// A message from a front-end user to the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inbound {
    /// Who sent it.
    pub from: UserAddress,
    /// Network-unique message id, used for deduplication.
    pub id: String,
    /// Plain text.
    pub text: String,
}

/// Correlates an outbound message with the agent message it carries, so the
/// router can advance that thread's delivery cursor once it is settled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAck {
    /// XMTP conversation id.
    pub thread: String,
    /// XMTP message id.
    pub message_id: String,
    /// The agent message's send time (ns), the cursor value.
    pub sent_at_ns: i64,
}

/// A message to a front-end user: an agent reply, or a bridge notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    /// Recipient.
    pub to: UserAddress,
    /// Plain text. The front end splits it to its network's size limit.
    pub text: String,
    /// Set for agent messages; `None` for bridge notices.
    pub ack: Option<DeliveryAck>,
}

/// What a front end (or the outbound pacer) reports to the router.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendEvent {
    /// A user sent a message.
    Message(Inbound),
    /// An agent message reached the user.
    Delivered(DeliveryAck),
    /// An agent message was given up on (send failures, queue overflow).
    Failed {
        /// The message.
        ack: DeliveryAck,
        /// Why, for logs.
        reason: String,
    },
}

/// Channels handed to a running front end.
#[derive(Debug)]
pub struct FrontendIo {
    /// Report inbound messages and delivery outcomes here.
    pub events: mpsc::Sender<FrontendEvent>,
    /// Messages to deliver, already rate-paced by the bridge.
    pub outbound: mpsc::Receiver<Outbound>,
    /// Flips to `true` on shutdown.
    pub shutdown: watch::Receiver<bool>,
}

/// Boxed future returned by [`Frontend::run`].
pub type FrontendFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

/// A messaging-network integration.
///
/// Contract:
/// - report every user message as [`FrontendEvent::Message`] with a stable,
///   network-unique id (redeliveries must reuse it);
/// - report only messages from other users (never the bridge's own sends);
/// - for every [`Outbound`] with an `ack`, report exactly one
///   [`FrontendEvent::Delivered`] or [`FrontendEvent::Failed`];
/// - return when `shutdown` flips to `true`.
pub trait Frontend: Send + 'static {
    /// Network id used in [`UserAddress::network`].
    fn network(&self) -> &str;
    /// Run until shutdown or a fatal error.
    fn run(self: Box<Self>, io: FrontendIo) -> FrontendFuture;
}

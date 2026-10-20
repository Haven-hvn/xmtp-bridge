//! Inbound admission: who may reach the agent, and how often.
//!
//! Anyone may message the bridge, so these buckets are its abuse controls:
//!
//! 1. **per sender** — one person cannot flood the agent;
//! 2. **global** — all senders together cannot exceed the agent's budget;
//! 3. **new senders** — each first message creates an XMTP group and an agent
//!    session, so how fast strangers may start conversations is capped
//!    separately.
//!
//! A denied message takes no token from any bucket, and the checks run in that
//! order so a sender who is over their own limit never spends global or
//! new-sender tokens.

use std::time::Instant;

use crate::bucket::{KeyedBuckets, Rate, TokenBucket};
use crate::message::UserAddress;

/// Inbound limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundLimits {
    /// Per-sender rate.
    pub per_sender: Rate,
    /// Rate across all senders.
    pub global: Rate,
    /// Rate of conversations started by senders without a session.
    pub new_senders: Rate,
    /// Most senders tracked at once (memory bound for per-sender buckets).
    pub max_tracked_senders: usize,
}

/// Why a message was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denied {
    /// The sender is over their own rate.
    Sender,
    /// The bridge as a whole is over its rate.
    Global,
    /// Too many new conversations are being started.
    NewSender,
}

/// Inbound token buckets.
#[derive(Debug, Clone)]
pub struct InboundLimiter {
    per_sender: KeyedBuckets<UserAddress>,
    global: TokenBucket,
    new_senders: TokenBucket,
}

impl InboundLimiter {
    /// Fresh (full) buckets.
    #[must_use]
    pub fn new(limits: InboundLimits, now: Instant) -> Self {
        Self {
            per_sender: KeyedBuckets::new(limits.per_sender, limits.max_tracked_senders),
            global: TokenBucket::new(limits.global, now),
            new_senders: TokenBucket::new(limits.new_senders, now),
        }
    }

    /// Admit one message from `from`, taking a token from every bucket that
    /// applies, or deny it and take none.
    ///
    /// # Errors
    /// [`Denied`] names the first bucket that was empty.
    pub fn admit(&mut self, from: &UserAddress, starts_conversation: bool, now: Instant) -> Result<(), Denied> {
        if !self.per_sender.bucket(from, now).can_take(now) {
            return Err(Denied::Sender);
        }
        if !self.global.can_take(now) {
            return Err(Denied::Global);
        }
        if starts_conversation && !self.new_senders.can_take(now) {
            return Err(Denied::NewSender);
        }
        self.per_sender.bucket(from, now).try_take(now);
        self.global.try_take(now);
        if starts_conversation {
            self.new_senders.try_take(now);
        }
        Ok(())
    }
}

/// Outbound-to-XMTP limits: how fast the bridge may call the XMTP network on
/// a user's behalf (`AgentRequest::Send`/`OpenThread`).
///
/// Unlike [`InboundLimits`], there is no "new senders" budget — opening a
/// thread is rate-limited the same as sending, since both are one call to
/// the XMTP worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XmtpOutboundLimits {
    /// Rate per Signal (front-end) user.
    pub per_user: Rate,
    /// Rate across all users together.
    pub global: Rate,
    /// Most users tracked at once (memory bound for per-user buckets).
    pub max_tracked_users: usize,
}

/// Why a call to the XMTP worker was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmtpDenied {
    /// The user is over their own rate to XMTP.
    User,
    /// The bridge as a whole is over its rate to XMTP.
    Global,
}

/// Token buckets limiting the bridge→XMTP direction.
#[derive(Debug, Clone)]
pub struct XmtpOutboundLimiter {
    per_user: KeyedBuckets<UserAddress>,
    global: TokenBucket,
}

impl XmtpOutboundLimiter {
    /// Fresh (full) buckets.
    #[must_use]
    pub fn new(limits: XmtpOutboundLimits, now: Instant) -> Self {
        Self {
            per_user: KeyedBuckets::new(limits.per_user, limits.max_tracked_users),
            global: TokenBucket::new(limits.global, now),
        }
    }

    /// Admit one call to XMTP on behalf of `user`, taking a token from every
    /// bucket that applies, or deny it and take none.
    ///
    /// # Errors
    /// [`XmtpDenied`] names the first bucket that was empty.
    pub fn admit(&mut self, user: &UserAddress, now: Instant) -> Result<(), XmtpDenied> {
        if !self.per_user.bucket(user, now).can_take(now) {
            return Err(XmtpDenied::User);
        }
        if !self.global.can_take(now) {
            return Err(XmtpDenied::Global);
        }
        self.per_user.bucket(user, now).try_take(now);
        self.global.try_take(now);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn limits() -> InboundLimits {
        InboundLimits {
            per_sender: Rate::new(2, 1, 10),
            global: Rate::new(3, 1, 10),
            new_senders: Rate::new(1, 1, 60),
            max_tracked_senders: 100,
        }
    }

    fn user(id: &str) -> UserAddress {
        UserAddress::new("signal", id)
    }

    #[test]
    fn per_sender_limit_applies_first_and_spends_nothing_else() {
        let t0 = Instant::now();
        let mut l = InboundLimiter::new(limits(), t0);
        assert_eq!(l.admit(&user("a"), false, t0), Ok(()));
        assert_eq!(l.admit(&user("a"), false, t0), Ok(()));
        // "a" is out; repeated attempts must not drain the global bucket.
        for _ in 0..10 {
            assert_eq!(l.admit(&user("a"), false, t0), Err(Denied::Sender));
        }
        assert_eq!(l.admit(&user("b"), false, t0), Ok(()), "one global token left for b");
        assert_eq!(l.admit(&user("c"), false, t0), Err(Denied::Global));
    }

    #[test]
    fn new_conversations_have_their_own_budget() {
        let t0 = Instant::now();
        let mut l = InboundLimiter::new(limits(), t0);
        assert_eq!(l.admit(&user("a"), true, t0), Ok(()));
        assert_eq!(l.admit(&user("b"), true, t0), Err(Denied::NewSender));
        // b was denied without spending its own or the global token.
        assert_eq!(l.admit(&user("b"), false, t0), Ok(()));
        assert_eq!(l.admit(&user("b"), true, t0 + Duration::from_secs(60)), Ok(()));
    }

    #[test]
    fn buckets_refill() {
        let t0 = Instant::now();
        let mut l = InboundLimiter::new(limits(), t0);
        assert!(l.admit(&user("a"), false, t0).is_ok());
        assert!(l.admit(&user("a"), false, t0).is_ok());
        assert_eq!(l.admit(&user("a"), false, t0), Err(Denied::Sender));
        assert!(l.admit(&user("a"), false, t0 + Duration::from_secs(10)).is_ok());
    }

    fn xmtp_limits() -> XmtpOutboundLimits {
        XmtpOutboundLimits { per_user: Rate::new(2, 1, 10), global: Rate::new(3, 1, 10), max_tracked_users: 100 }
    }

    #[test]
    fn xmtp_per_user_limit_applies_first_and_spends_nothing_else() {
        let t0 = Instant::now();
        let mut l = XmtpOutboundLimiter::new(xmtp_limits(), t0);
        assert_eq!(l.admit(&user("a"), t0), Ok(()));
        assert_eq!(l.admit(&user("a"), t0), Ok(()));
        // "a" is out; repeated attempts must not drain the global bucket.
        for _ in 0..10 {
            assert_eq!(l.admit(&user("a"), t0), Err(XmtpDenied::User));
        }
        assert_eq!(l.admit(&user("b"), t0), Ok(()), "one global token left for b");
        assert_eq!(l.admit(&user("c"), t0), Err(XmtpDenied::Global));
    }

    #[test]
    fn xmtp_buckets_refill() {
        let t0 = Instant::now();
        let mut l = XmtpOutboundLimiter::new(xmtp_limits(), t0);
        assert!(l.admit(&user("a"), t0).is_ok());
        assert!(l.admit(&user("a"), t0).is_ok());
        assert_eq!(l.admit(&user("a"), t0), Err(XmtpDenied::User));
        assert!(l.admit(&user("a"), t0 + Duration::from_secs(10)).is_ok());
    }
}

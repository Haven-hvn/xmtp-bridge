//! Outbound pacing: keep the bridge's own sending under each network's limits.
//!
//! Messages to users are queued, not dropped, while a bucket is empty: an
//! agent reply that took minutes to produce should still arrive. Each
//! recipient has a bounded queue, recipients are served round-robin so one
//! busy conversation cannot starve the others, and a per-recipient plus a
//! network-wide bucket must both have a token before anything is sent.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::bucket::{KeyedBuckets, Rate, TokenBucket};
use crate::message::{Outbound, UserAddress};

/// Outbound limits for one front-end network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundLimits {
    /// Per-recipient rate.
    pub per_recipient: Rate,
    /// Rate across all recipients.
    pub global: Rate,
    /// Most messages queued for one recipient; beyond it, new ones are dropped.
    pub max_queue_per_recipient: usize,
    /// Most messages queued in total.
    pub max_queue_total: usize,
}

/// Queue + buckets for one network. Pure: time is passed in.
#[derive(Debug)]
pub struct Pacer {
    limits: OutboundLimits,
    per_recipient: KeyedBuckets<UserAddress>,
    global: TokenBucket,
    queues: HashMap<UserAddress, VecDeque<Outbound>>,
    /// Recipients with a non-empty queue, in service order.
    ready: VecDeque<UserAddress>,
    total: usize,
}

impl Pacer {
    /// Empty queues, full buckets.
    #[must_use]
    pub fn new(limits: OutboundLimits, now: Instant) -> Self {
        Self {
            limits,
            // Every queued recipient must keep its bucket, so track at least as
            // many keys as messages can be queued.
            per_recipient: KeyedBuckets::new(limits.per_recipient, limits.max_queue_total.max(1) * 2),
            global: TokenBucket::new(limits.global, now),
            queues: HashMap::new(),
            ready: VecDeque::new(),
            total: 0,
        }
    }

    /// Queue a message.
    ///
    /// # Errors
    /// Returns the message back when its recipient's queue or the total queue
    /// is full.
    #[allow(
        clippy::result_large_err,
        reason = "hands the caller's own message back; boxing would only add an allocation"
    )]
    pub fn push(&mut self, message: Outbound) -> Result<(), Outbound> {
        if self.total >= self.limits.max_queue_total {
            return Err(message);
        }
        let queue = self.queues.entry(message.to.clone()).or_default();
        if queue.len() >= self.limits.max_queue_per_recipient {
            return Err(message);
        }
        if queue.is_empty() {
            self.ready.push_back(message.to.clone());
        }
        queue.push_back(message);
        self.total += 1;
        Ok(())
    }

    /// The next message whose buckets allow sending at `now`, if any.
    pub fn pop_ready(&mut self, now: Instant) -> Option<Outbound> {
        if !self.global.can_take(now) {
            return None;
        }
        for _ in 0..self.ready.len() {
            let recipient = self.ready.pop_front()?;
            if !self.per_recipient.bucket(&recipient, now).try_take(now) {
                self.ready.push_back(recipient);
                continue;
            }
            self.global.try_take(now);
            let queue = self.queues.get_mut(&recipient)?;
            let message = queue.pop_front();
            if queue.is_empty() {
                self.queues.remove(&recipient);
            } else {
                self.ready.push_back(recipient);
            }
            self.total -= 1;
            return message;
        }
        None
    }

    /// How long until [`Self::pop_ready`] could return something; `None` when
    /// nothing is queued.
    pub fn next_ready_in(&mut self, now: Instant) -> Option<Duration> {
        if self.ready.is_empty() {
            return None;
        }
        let global = self.global.time_until_available(now);
        let soonest = self
            .ready
            .clone()
            .iter()
            .map(|r| self.per_recipient.bucket(r, now).time_until_available(now))
            .min()
            .unwrap_or(Duration::ZERO);
        Some(global.max(soonest))
    }

    /// Messages currently queued.
    #[must_use]
    pub const fn queued(&self) -> usize {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> OutboundLimits {
        OutboundLimits {
            per_recipient: Rate::new(1, 1, 1),
            global: Rate::new(10, 10, 1),
            max_queue_per_recipient: 3,
            max_queue_total: 5,
        }
    }

    fn msg(to: &str, text: &str) -> Outbound {
        Outbound { to: UserAddress::new("signal", to), text: text.into(), ack: None }
    }

    fn drain(p: &mut Pacer, now: Instant) -> Vec<String> {
        std::iter::from_fn(|| p.pop_ready(now)).map(|m| format!("{}:{}", m.to.user, m.text)).collect()
    }

    #[test]
    fn paces_each_recipient_and_preserves_order() {
        let t0 = Instant::now();
        let mut p = Pacer::new(limits(), t0);
        p.push(msg("a", "1")).unwrap();
        p.push(msg("a", "2")).unwrap();
        assert_eq!(drain(&mut p, t0), vec!["a:1"]);
        assert_eq!(p.next_ready_in(t0), Some(Duration::from_secs(1)));
        assert_eq!(drain(&mut p, t0 + Duration::from_secs(1)), vec!["a:2"]);
        assert_eq!(p.next_ready_in(t0), None);
    }

    #[test]
    fn serves_recipients_round_robin() {
        let t0 = Instant::now();
        let mut p = Pacer::new(limits(), t0);
        p.push(msg("a", "1")).unwrap();
        p.push(msg("a", "2")).unwrap();
        p.push(msg("b", "1")).unwrap();
        assert_eq!(drain(&mut p, t0), vec!["a:1", "b:1"], "b is not stuck behind a");
    }

    #[test]
    fn bounded_queues_hand_back_overflow() {
        let t0 = Instant::now();
        let mut p = Pacer::new(limits(), t0);
        for i in 0..3 {
            p.push(msg("a", &i.to_string())).unwrap();
        }
        assert!(p.push(msg("a", "x")).is_err(), "per-recipient cap");
        p.push(msg("b", "1")).unwrap();
        p.push(msg("c", "1")).unwrap();
        assert!(p.push(msg("d", "1")).is_err(), "total cap");
        assert_eq!(p.queued(), 5);
    }

    #[test]
    fn global_bucket_gates_everyone() {
        let t0 = Instant::now();
        let mut p = Pacer::new(OutboundLimits { global: Rate::new(1, 1, 2), ..limits() }, t0);
        p.push(msg("a", "1")).unwrap();
        p.push(msg("b", "1")).unwrap();
        assert_eq!(drain(&mut p, t0), vec!["a:1"]);
        assert_eq!(p.next_ready_in(t0), Some(Duration::from_secs(2)));
        assert_eq!(drain(&mut p, t0 + Duration::from_secs(2)), vec!["b:1"]);
    }
}

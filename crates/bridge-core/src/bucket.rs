//! Token buckets.
//!
//! A bucket holds up to `burst` tokens and refills continuously at
//! `tokens / per_secs` tokens per second. An action proceeds only when a whole
//! token is available, and taking it costs one token. Time is always passed in
//! (`now`), never read from a clock, so every decision is deterministic and
//! testable.

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Float slack so accumulated refill rounding never strands a bucket at
/// 0.999999… tokens.
const EPSILON: f64 = 1e-9;

/// A refill rate plus burst size: "`tokens` every `per_secs` seconds, at most
/// `burst` saved up".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rate {
    /// Bucket capacity: how many actions may happen back to back.
    pub burst: u32,
    /// Tokens added per `per_secs` window.
    pub tokens: u32,
    /// Window length in seconds.
    pub per_secs: u64,
}

impl Rate {
    /// Construct a rate.
    #[must_use]
    pub const fn new(burst: u32, tokens: u32, per_secs: u64) -> Self {
        Self { burst, tokens, per_secs }
    }

    /// Reject rates that would never refill or never admit anything.
    ///
    /// # Errors
    /// When any field is zero.
    pub fn validate(&self, name: &str) -> Result<(), String> {
        if self.burst == 0 || self.tokens == 0 || self.per_secs == 0 {
            return Err(format!("{name}: burst, tokens and per_secs must all be at least 1"));
        }
        Ok(())
    }

    fn refill_per_sec(&self) -> f64 {
        f64::from(self.tokens) / self.per_secs as f64
    }
}

/// One token bucket.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    refill_per_sec: f64,
    tokens: f64,
    updated: Instant,
}

impl TokenBucket {
    /// A full bucket.
    #[must_use]
    pub fn new(rate: Rate, now: Instant) -> Self {
        let capacity = f64::from(rate.burst);
        Self { capacity, refill_per_sec: rate.refill_per_sec(), tokens: capacity, updated: now }
    }

    fn refill(&mut self, now: Instant) {
        // `now` earlier than the last update (caller clock skew) refills nothing.
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.updated = now;
        }
    }

    /// Tokens available at `now` (fractional).
    pub fn available(&mut self, now: Instant) -> f64 {
        self.refill(now);
        self.tokens
    }

    /// Whether one token is available, without taking it.
    pub fn can_take(&mut self, now: Instant) -> bool {
        self.available(now) + EPSILON >= 1.0
    }

    /// Take one token if available.
    pub fn try_take(&mut self, now: Instant) -> bool {
        if self.can_take(now) {
            self.tokens = (self.tokens - 1.0).max(0.0);
            true
        } else {
            false
        }
    }

    /// Whether the bucket is full. A full bucket is indistinguishable from a
    /// fresh one, so it can be forgotten without changing any decision.
    pub fn is_full(&mut self, now: Instant) -> bool {
        self.available(now) + EPSILON >= self.capacity
    }

    /// How long until one token is available (zero when one already is).
    /// Rounded up to whole milliseconds so a waiter never wakes just early
    /// and spins.
    pub fn time_until_available(&mut self, now: Instant) -> Duration {
        let missing = 1.0 - self.available(now);
        if missing <= EPSILON {
            return Duration::ZERO;
        }
        // Shave float noise before rounding up (1/3-rate buckets would
        // otherwise compute 2000.0000001 ms → 2001 ms), but never return zero
        // while no token is available, or a waiter would spin.
        let ms = (missing / self.refill_per_sec * 1000.0 - 1e-6).ceil().max(1.0);
        Duration::from_millis(ms as u64)
    }
}

#[derive(Debug, Clone)]
struct Entry {
    bucket: TokenBucket,
    last_used: Instant,
}

/// One bucket per key (per sender, per recipient), bounded in memory.
///
/// When the key limit is reached, full buckets are dropped first (lossless:
/// a fresh bucket is also full). Only if every tracked key is mid-refill is
/// the least recently used one dropped; that key then restarts with a full
/// bucket, which is why the global bucket should always sit behind this one.
#[derive(Debug, Clone)]
pub struct KeyedBuckets<K> {
    rate: Rate,
    max_keys: usize,
    map: HashMap<K, Entry>,
}

impl<K: Eq + Hash + Clone> KeyedBuckets<K> {
    /// Track at most `max_keys` keys (minimum 1).
    #[must_use]
    pub fn new(rate: Rate, max_keys: usize) -> Self {
        Self { rate, max_keys: max_keys.max(1), map: HashMap::new() }
    }

    /// The bucket for `key`, created full if unknown.
    pub fn bucket(&mut self, key: &K, now: Instant) -> &mut TokenBucket {
        if !self.map.contains_key(key) {
            if self.map.len() >= self.max_keys {
                self.prune(now);
            }
            self.map.insert(key.clone(), Entry { bucket: TokenBucket::new(self.rate, now), last_used: now });
        }
        let entry = self.map.get_mut(key).expect("entry inserted above");
        entry.last_used = now;
        &mut entry.bucket
    }

    /// Number of tracked keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether no keys are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn prune(&mut self, now: Instant) {
        self.map.retain(|_, entry| !entry.bucket.is_full(now));
        while self.map.len() >= self.max_keys {
            let Some(oldest) = self.map.iter().min_by_key(|(_, e)| e.last_used).map(|(k, _)| k.clone()) else {
                break;
            };
            self.map.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    #[test]
    fn starts_full_and_drains() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(Rate::new(3, 1, 1), t0);
        assert!(b.try_take(t0));
        assert!(b.try_take(t0));
        assert!(b.try_take(t0));
        assert!(!b.try_take(t0), "burst of 3 exhausted");
    }

    #[test]
    fn refills_at_rate_and_caps_at_burst() {
        let t0 = Instant::now();
        // 5 tokens per 60 s = one token every 12 s.
        let mut b = TokenBucket::new(Rate::new(2, 5, 60), t0);
        assert!(b.try_take(t0) && b.try_take(t0));
        assert!(!b.can_take(t0 + secs(11.9)));
        assert!(b.try_take(t0 + secs(12.0)));
        // A long idle period refills to the burst, never beyond.
        let later = t0 + secs(3600.0);
        assert!((b.available(later) - 2.0).abs() < 1e-6);
        assert!(b.is_full(later));
    }

    #[test]
    fn time_until_available_is_exact_and_rounded_up() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(Rate::new(1, 1, 3), t0);
        assert_eq!(b.time_until_available(t0), Duration::ZERO);
        assert!(b.try_take(t0));
        assert_eq!(b.time_until_available(t0), Duration::from_millis(3000));
        assert_eq!(b.time_until_available(t0 + secs(1.0)), Duration::from_millis(2000));
        let t1 = t0 + secs(1.0);
        let wait = b.time_until_available(t1);
        assert!(!b.can_take(t1 + wait - Duration::from_millis(5)), "not early");
        assert!(b.try_take(t1 + wait), "on time");
    }

    #[test]
    fn clock_going_backwards_refills_nothing() {
        let t0 = Instant::now() + secs(10.0);
        let mut b = TokenBucket::new(Rate::new(1, 1, 1), t0);
        assert!(b.try_take(t0));
        assert!(!b.can_take(t0 - secs(5.0)));
    }

    #[test]
    fn many_small_refills_add_up_to_whole_tokens() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(Rate::new(1, 1, 1), t0);
        assert!(b.try_take(t0));
        let mut now = t0;
        for _ in 0..10 {
            now += secs(0.1);
            let _ = b.available(now);
        }
        assert!(b.try_take(now), "ten 100 ms refills make one token");
    }

    #[test]
    fn rate_validation_rejects_zeros() {
        assert!(Rate::new(0, 1, 1).validate("x").is_err());
        assert!(Rate::new(1, 0, 1).validate("x").is_err());
        assert!(Rate::new(1, 1, 0).validate("x").is_err());
        assert!(Rate::new(1, 1, 1).validate("x").is_ok());
    }

    #[test]
    fn keyed_buckets_are_independent() {
        let t0 = Instant::now();
        let mut kb = KeyedBuckets::new(Rate::new(1, 1, 60), 10);
        assert!(kb.bucket(&"a", t0).try_take(t0));
        assert!(!kb.bucket(&"a", t0).try_take(t0));
        assert!(kb.bucket(&"b", t0).try_take(t0), "b has its own bucket");
    }

    #[test]
    fn keyed_buckets_prune_full_buckets_first() {
        let t0 = Instant::now();
        let mut kb = KeyedBuckets::new(Rate::new(1, 1, 10), 2);
        assert!(kb.bucket(&"busy", t0).try_take(t0)); // mid-refill
        let _ = kb.bucket(&"idle", t0); // full, never used
        let _ = kb.bucket(&"new", t0); // over the limit: prune
        assert_eq!(kb.len(), 2);
        // "busy" kept its empty bucket: pruning dropped the full "idle" one.
        assert!(!kb.bucket(&"busy", t0).can_take(t0));
    }

    #[test]
    fn keyed_buckets_fall_back_to_lru() {
        let t0 = Instant::now();
        let mut kb = KeyedBuckets::new(Rate::new(1, 1, 100), 2);
        assert!(kb.bucket(&"old", t0).try_take(t0));
        assert!(kb.bucket(&"recent", t0 + secs(1.0)).try_take(t0 + secs(1.0)));
        assert!(kb.bucket(&"new", t0 + secs(2.0)).try_take(t0 + secs(2.0)));
        assert_eq!(kb.len(), 2);
        // "recent" survived with its spent token; "old" was evicted.
        assert!(!kb.bucket(&"recent", t0 + secs(2.0)).can_take(t0 + secs(2.0)));
    }
}

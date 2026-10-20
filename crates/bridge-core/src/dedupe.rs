//! Bounded "seen" set for message-id deduplication.

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

/// Remembers the most recent `capacity` ids; the oldest is forgotten first.
#[derive(Debug, Clone)]
pub struct Dedupe<T> {
    capacity: usize,
    set: HashSet<T>,
    order: VecDeque<T>,
}

impl<T: Eq + Hash + Clone> Dedupe<T> {
    /// A set remembering at most `capacity` ids (minimum 1).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), set: HashSet::new(), order: VecDeque::new() }
    }

    /// Record `id`; returns `false` when it was already seen.
    pub fn insert(&mut self, id: T) -> bool {
        if self.set.contains(&id) {
            return false;
        }
        if self.order.len() >= self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
        }
        self.set.insert(id.clone());
        self.order.push_back(id);
        true
    }

    /// Number of remembered ids.
    #[must_use]
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Whether nothing is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_repeats_and_forgets_oldest() {
        let mut d = Dedupe::new(2);
        assert!(d.insert("a"));
        assert!(!d.insert("a"));
        assert!(d.insert("b"));
        assert!(d.insert("c")); // evicts "a"
        assert_eq!(d.len(), 2);
        assert!(d.insert("a"), "a was forgotten");
        assert!(!d.insert("c"));
    }
}

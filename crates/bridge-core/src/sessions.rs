//! The persisted map from front-end users to their XMTP thread with the agent.
//!
//! Each session also carries a delivery cursor: the send time of the newest
//! agent message already settled for that user. After a restart the worker
//! re-reads every thread past its cursor, so replies produced while the
//! bridge was down still arrive.
//!
//! The map is bounded: inserting beyond `max_sessions` evicts the least
//! recently active user. An evicted user who writes again gets a new thread,
//! and with it a fresh agent session.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::message::UserAddress;
use crate::protocol::ResumeCursor;

const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Session {
    thread: String,
    cursor_ns: i64,
    /// Monotonic activity counter (not wall time) used for LRU order.
    last_active: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateFile {
    version: u32,
    sessions: Vec<Record>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    user: UserAddress,
    thread: String,
    cursor_ns: i64,
    last_active: u64,
}

/// User ↔ thread map with cursors and LRU eviction.
#[derive(Debug)]
pub struct SessionStore {
    max_sessions: usize,
    by_user: HashMap<UserAddress, Session>,
    by_thread: HashMap<String, UserAddress>,
    clock: u64,
    dirty: bool,
}

impl SessionStore {
    /// An empty store holding at most `max_sessions` sessions (minimum 1).
    #[must_use]
    pub fn new(max_sessions: usize) -> Self {
        Self {
            max_sessions: max_sessions.max(1),
            by_user: HashMap::new(),
            by_thread: HashMap::new(),
            clock: 0,
            dirty: false,
        }
    }

    /// Load from `path`, or start empty when it does not exist.
    ///
    /// # Errors
    /// Unreadable or malformed state, or an unknown version.
    pub fn load(path: &Path, max_sessions: usize) -> anyhow::Result<Self> {
        let mut store = Self::new(max_sessions);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(store),
            Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
        };
        let file: StateFile = serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?;
        anyhow::ensure!(
            file.version == STATE_VERSION,
            "unsupported state version {} in {}",
            file.version,
            path.display()
        );
        let mut records = file.sessions;
        // Most recent last, so a lowered max_sessions keeps the newest.
        records.sort_by_key(|r| r.last_active);
        for record in records {
            store.clock = store.clock.max(record.last_active);
            store.put(
                record.user,
                Session { thread: record.thread, cursor_ns: record.cursor_ns, last_active: record.last_active },
            );
        }
        store.dirty = false;
        Ok(store)
    }

    /// Write atomically (temp file, fsync, rename) with owner-only permissions.
    ///
    /// # Errors
    /// I/O failures.
    pub fn save(&mut self, path: &Path) -> anyhow::Result<()> {
        let mut sessions: Vec<Record> = self
            .by_user
            .iter()
            .map(|(user, s)| Record {
                user: user.clone(),
                thread: s.thread.clone(),
                cursor_ns: s.cursor_ns,
                last_active: s.last_active,
            })
            .collect();
        sessions.sort_by(|a, b| a.user.cmp(&b.user));
        let json = serde_json::to_vec_pretty(&StateFile { version: STATE_VERSION, sessions })?;
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        let tmp = tmp_path(path);
        // A leftover temp file could carry wider permissions; start fresh.
        let _ = fs::remove_file(&tmp);
        {
            let mut file = open_private(&tmp)?;
            file.write_all(&json)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))?;
        self.dirty = false;
        Ok(())
    }

    /// Whether there are unsaved changes.
    #[must_use]
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Number of sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_user.len()
    }

    /// Whether there are no sessions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_user.is_empty()
    }

    /// The user's thread, if they have a session.
    #[must_use]
    pub fn thread_for(&self, user: &UserAddress) -> Option<&str> {
        self.by_user.get(user).map(|s| s.thread.as_str())
    }

    /// The user a thread belongs to.
    #[must_use]
    pub fn user_for(&self, thread: &str) -> Option<&UserAddress> {
        self.by_thread.get(thread)
    }

    /// A thread's delivery cursor.
    #[must_use]
    pub fn cursor(&self, thread: &str) -> Option<i64> {
        let user = self.by_thread.get(thread)?;
        self.by_user.get(user).map(|s| s.cursor_ns)
    }

    /// Mark the user as active now (LRU order).
    pub fn touch(&mut self, user: &UserAddress) {
        self.clock += 1;
        let clock = self.clock;
        if let Some(session) = self.by_user.get_mut(user) {
            session.last_active = clock;
            self.dirty = true;
        }
    }

    /// Record a new session; returns the users evicted to make room.
    pub fn insert(&mut self, user: UserAddress, thread: String) -> Vec<UserAddress> {
        self.clock += 1;
        let session = Session { thread, cursor_ns: 0, last_active: self.clock };
        let evicted = self.put(user, session);
        self.dirty = true;
        evicted
    }

    /// Advance a thread's cursor (never backwards).
    pub fn advance_cursor(&mut self, thread: &str, sent_at_ns: i64) {
        let Some(user) = self.by_thread.get(thread) else { return };
        if let Some(session) = self.by_user.get_mut(user)
            && sent_at_ns > session.cursor_ns
        {
            session.cursor_ns = sent_at_ns;
            self.dirty = true;
        }
    }

    /// Every thread's cursor, for [`crate::protocol::AgentRequest::Resume`].
    #[must_use]
    pub fn cursors(&self) -> Vec<ResumeCursor> {
        let mut cursors: Vec<ResumeCursor> =
            self.by_user.values().map(|s| ResumeCursor { thread: s.thread.clone(), after_ns: s.cursor_ns }).collect();
        cursors.sort_by(|a, b| a.thread.cmp(&b.thread));
        cursors
    }

    fn put(&mut self, user: UserAddress, session: Session) -> Vec<UserAddress> {
        if let Some(old) = self.by_user.remove(&user) {
            self.by_thread.remove(&old.thread);
        }
        let mut evicted = Vec::new();
        while self.by_user.len() >= self.max_sessions {
            let Some(oldest) = self.by_user.iter().min_by_key(|(_, s)| s.last_active).map(|(u, _)| u.clone()) else {
                break;
            };
            if let Some(old) = self.by_user.remove(&oldest) {
                self.by_thread.remove(&old.thread);
            }
            evicted.push(oldest);
        }
        self.by_thread.insert(session.thread.clone(), user.clone());
        self.by_user.insert(user, session);
        evicted
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(std::ffi::OsStr::to_os_string).unwrap_or_default();
    name.push(".tmp");
    path.with_file_name(name)
}

#[cfg(unix)]
fn open_private(path: &Path) -> anyhow::Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("writing {}", path.display()))
}

#[cfg(not(unix))]
fn open_private(path: &Path) -> anyhow::Result<fs::File> {
    fs::File::create(path).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(id: &str) -> UserAddress {
        UserAddress::new("signal", id)
    }

    #[test]
    fn maps_both_ways_and_evicts_least_recent() {
        let mut s = SessionStore::new(2);
        assert!(s.insert(user("a"), "ta".into()).is_empty());
        assert!(s.insert(user("b"), "tb".into()).is_empty());
        s.touch(&user("a"));
        assert_eq!(s.insert(user("c"), "tc".into()), vec![user("b")]);
        assert_eq!(s.thread_for(&user("a")), Some("ta"));
        assert_eq!(s.user_for("tc"), Some(&user("c")));
        assert_eq!(s.user_for("tb"), None, "evicted thread is unmapped");
    }

    #[test]
    fn cursors_only_move_forward() {
        let mut s = SessionStore::new(5);
        s.insert(user("a"), "ta".into());
        s.advance_cursor("ta", 10);
        s.advance_cursor("ta", 5);
        assert_eq!(s.cursor("ta"), Some(10));
        s.advance_cursor("unknown", 99);
    }

    #[test]
    fn save_and_load_round_trip() {
        let dir = std::env::temp_dir().join(format!("bridge-core-test-{}", std::process::id()));
        let path = dir.join("state.json");
        let mut s = SessionStore::new(5);
        s.insert(user("a"), "ta".into());
        s.insert(user("b"), "tb".into());
        s.advance_cursor("tb", 42);
        s.save(&path).unwrap();
        assert!(!s.is_dirty());

        let loaded = SessionStore::load(&path, 5).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.cursor("tb"), Some(42));
        assert_eq!(loaded.user_for("ta"), Some(&user("a")));

        // A smaller limit keeps the most recently active sessions.
        let trimmed = SessionStore::load(&path, 1).unwrap();
        assert_eq!(trimmed.len(), 1);
        assert_eq!(trimmed.thread_for(&user("b")), Some("tb"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o077, 0, "state file is owner-only");
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn missing_file_loads_empty() {
        let s = SessionStore::load(Path::new("/nonexistent/xmtp-bridge/state.json"), 3).unwrap();
        assert!(s.is_empty());
    }
}

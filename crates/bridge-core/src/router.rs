//! The bridge's state machine: admission, session routing, and delivery
//! bookkeeping. Pure: every handler takes `now` and returns [`Action`]s;
//! [`crate::runtime`] performs them.
//!
//! Inbound path: dedupe → length check → [`InboundLimiter`] → the user's
//! thread (opening one on first contact) → [`AgentRequest::Send`].
//! Outbound path: [`AgentEvent::Message`] → the thread's user → [`Outbound`]
//! (the pacer and front end take it from there) → the delivery cursor
//! advances when the front end settles it.
//!
//! Requests to the worker are only emitted while it is ready. Anything that
//! is not yet acknowledged (thread opens, sends) is kept and re-sent when a
//! restarted worker reports [`AgentEvent::Ready`], up to
//! [`RouterConfig::max_send_attempts`] times. A worker that dies after
//! sending but before acknowledging can therefore cause one duplicate
//! message to the agent; nothing is silently lost.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use crate::bucket::{KeyedBuckets, Rate};
use crate::dedupe::Dedupe;
use crate::limiter::{Denied, InboundLimiter, InboundLimits, XmtpOutboundLimiter, XmtpOutboundLimits};
use crate::message::{DeliveryAck, FrontendEvent, Inbound, Outbound, UserAddress};
use crate::protocol::{AgentEvent, AgentRequest};
use crate::sessions::SessionStore;
use crate::text::char_len;

/// Texts the bridge itself sends to users.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notices {
    /// The sender is over their own rate.
    pub rate_limited: String,
    /// The bridge as a whole is over its rate.
    pub busy: String,
    /// Too many new conversations right now.
    pub new_sender_busy: String,
    /// Message too long; `{max}` is replaced by the limit.
    pub too_long: String,
    /// The agent link could not open a conversation or deliver a message.
    pub agent_unavailable: String,
    /// More messages arrived than are buffered while a conversation opens.
    pub still_connecting: String,
    /// The agent link is down; the message is held and delivered later.
    pub agent_offline: String,
    /// The bridge is sending to XMTP faster than its own outbound rate allows.
    pub xmtp_rate_limited: String,
}

impl Default for Notices {
    fn default() -> Self {
        Self {
            rate_limited: "You're sending messages faster than I can take them. Please wait a minute and try again."
                .into(),
            busy: "I'm handling a lot of messages right now. Please try again in a few minutes.".into(),
            new_sender_busy: "I can't start new conversations right now. Please try again later.".into(),
            too_long: "That message is too long. Please keep it under {max} characters.".into(),
            agent_unavailable:
                "I couldn't reach the agent just now, so your last message wasn't delivered. Please try again later."
                    .into(),
            still_connecting: "Still connecting you to the agent. Please wait a moment before sending more.".into(),
            agent_offline: "The agent is offline right now. I'll pass your message on when it's back.".into(),
            xmtp_rate_limited: "I'm relaying messages to the agent faster than XMTP allows right now. Please wait a moment and try again."
                .into(),
        }
    }
}

/// Router configuration.
#[derive(Debug, Clone)]
pub struct RouterConfig {
    /// Inbound buckets.
    pub limits: InboundLimits,
    /// Outbound-to-XMTP buckets (how fast the bridge may call the XMTP
    /// worker on a user's behalf).
    pub xmtp_limits: XmtpOutboundLimits,
    /// Most user sessions kept (LRU beyond).
    pub max_sessions: usize,
    /// Longest accepted inbound message, in characters.
    pub max_message_chars: usize,
    /// At most one notice per user per this many seconds.
    pub notice_cooldown_secs: u64,
    /// Messages buffered per user while their thread is being created.
    pub max_queued_while_opening: usize,
    /// Times an unacknowledged request is sent before giving up.
    pub max_send_attempts: u32,
    /// Most unacknowledged sends held (they pile up while the worker is down).
    pub max_inflight: usize,
    /// Message ids remembered for deduplication (each direction).
    pub dedupe_capacity: usize,
    /// Whether admitted messages are held and delivered later while the agent
    /// link is down (`true`), or dropped with a notice (`false`). Live rooms
    /// want `false`: nobody wants an answer to a question from ten minutes
    /// ago.
    pub hold_while_offline: bool,
    /// Notice texts.
    pub notices: Notices,
}

/// Something the runtime must do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Write a request to the XMTP worker.
    Agent(AgentRequest),
    /// Hand a message to a front end (through its pacer).
    Frontend(Outbound),
}

/// Input from the agent worker's supervisor (see [`crate::worker`]).
pub type AgentInput = crate::worker::WorkerInput<AgentEvent>;

#[derive(Debug)]
struct PendingOpen {
    req: u64,
    queued: Vec<String>,
    attempts: u32,
}

#[derive(Debug)]
struct InflightSend {
    user: UserAddress,
    thread: String,
    text: String,
    attempts: u32,
}

/// The state machine.
#[derive(Debug)]
pub struct Router {
    cfg: RouterConfig,
    limiter: InboundLimiter,
    xmtp_limiter: XmtpOutboundLimiter,
    notice_buckets: KeyedBuckets<UserAddress>,
    sessions: SessionStore,
    seen_inbound: Dedupe<(UserAddress, String)>,
    seen_agent: Dedupe<String>,
    pending_open: HashMap<UserAddress, PendingOpen>,
    open_reqs: BTreeMap<u64, UserAddress>,
    inflight: BTreeMap<u64, InflightSend>,
    next_req: u64,
    ready: Option<u64>,
}

impl Router {
    /// A router over existing sessions.
    #[must_use]
    pub fn new(cfg: RouterConfig, sessions: SessionStore, now: Instant) -> Self {
        let notice_rate = Rate::new(1, 1, cfg.notice_cooldown_secs.max(1));
        Self {
            limiter: InboundLimiter::new(cfg.limits, now),
            xmtp_limiter: XmtpOutboundLimiter::new(cfg.xmtp_limits, now),
            notice_buckets: KeyedBuckets::new(notice_rate, cfg.limits.max_tracked_senders),
            sessions,
            seen_inbound: Dedupe::new(cfg.dedupe_capacity),
            seen_agent: Dedupe::new(cfg.dedupe_capacity),
            pending_open: HashMap::new(),
            open_reqs: BTreeMap::new(),
            inflight: BTreeMap::new(),
            next_req: 1,
            ready: None,
            cfg,
        }
    }

    /// The generation of the ready worker, if one is ready.
    #[must_use]
    pub const fn agent_generation(&self) -> Option<u64> {
        self.ready
    }

    /// The session store (for saving).
    pub const fn sessions_mut(&mut self) -> &mut SessionStore {
        &mut self.sessions
    }

    /// Handle a front-end event.
    pub fn on_frontend(&mut self, event: FrontendEvent, now: Instant) -> Vec<Action> {
        match event {
            FrontendEvent::Message(inbound) => self.on_inbound(inbound, now),
            FrontendEvent::Delivered(ack) => {
                self.settle(&ack);
                Vec::new()
            }
            FrontendEvent::Failed { ack, reason } => {
                tracing::warn!(thread = %short(&ack.thread), %reason, "agent message not delivered");
                self.settle(&ack);
                Vec::new()
            }
        }
    }

    /// Handle worker input.
    pub fn on_agent(&mut self, input: AgentInput, now: Instant) -> Vec<Action> {
        match input {
            AgentInput::Down { generation } => {
                if self.ready == Some(generation) {
                    tracing::warn!(generation, "agent link down");
                    self.ready = None;
                }
                Vec::new()
            }
            AgentInput::Event { generation, event: AgentEvent::Ready { inbox_id, agent_inbox_id, .. } } => {
                tracing::info!(generation, inbox = %short(&inbox_id), agent = %short(&agent_inbox_id), "agent link ready");
                self.ready = Some(generation);
                self.replay(now)
            }
            AgentInput::Event { generation, event } => {
                if self.ready != Some(generation) {
                    tracing::debug!(generation, "ignoring event from a stale worker");
                    return Vec::new();
                }
                self.on_agent_event(event, now)
            }
        }
    }

    fn on_inbound(&mut self, inbound: Inbound, now: Instant) -> Vec<Action> {
        let Inbound { from, id, text } = inbound;
        if !self.seen_inbound.insert((from.clone(), id)) {
            return Vec::new();
        }
        let text = text.trim();
        if text.is_empty() {
            return Vec::new();
        }
        if char_len(text) > self.cfg.max_message_chars {
            let notice = self.cfg.notices.too_long.replace("{max}", &self.cfg.max_message_chars.to_string());
            return self.notice(&from, notice, now);
        }
        let has_session = self.sessions.thread_for(&from).is_some();
        let opening = self.pending_open.contains_key(&from);
        if let Err(denied) = self.limiter.admit(&from, !has_session && !opening, now) {
            tracing::info!(user = %from, ?denied, "rate limited");
            let notice = match denied {
                Denied::Sender => self.cfg.notices.rate_limited.clone(),
                Denied::Global => self.cfg.notices.busy.clone(),
                Denied::NewSender => self.cfg.notices.new_sender_busy.clone(),
            };
            return self.notice(&from, notice, now);
        }
        if self.inflight.len() >= self.cfg.max_inflight {
            tracing::warn!(user = %from, "too many undelivered messages held; dropping");
            let notice = self.cfg.notices.agent_unavailable.clone();
            return self.notice(&from, notice, now);
        }
        if self.ready.is_none() && !self.cfg.hold_while_offline {
            tracing::info!(user = %from, "agent link down; dropping (hold_while_offline = false)");
            let notice = self.cfg.notices.agent_offline.clone();
            return self.notice(&from, notice, now);
        }
        let mut actions = self.route(from.clone(), text.to_owned(), now);
        if self.ready.is_none() {
            let notice = self.cfg.notices.agent_offline.clone();
            actions.extend(self.notice(&from, notice, now));
        }
        actions
    }

    /// Send an admitted message on the user's thread, opening one if needed.
    fn route(&mut self, from: UserAddress, text: String, now: Instant) -> Vec<Action> {
        if let Some(thread) = self.sessions.thread_for(&from).map(str::to_owned) {
            self.sessions.touch(&from);
            return self.queue_send(from, thread, text, now);
        }
        if let Some(pending) = self.pending_open.get_mut(&from) {
            if pending.queued.len() >= self.cfg.max_queued_while_opening {
                let notice = self.cfg.notices.still_connecting.clone();
                return self.notice(&from, notice, now);
            }
            pending.queued.push(text);
            return Vec::new();
        }
        if let Err(denied) = self.xmtp_limiter.admit(&from, now) {
            tracing::info!(user = %from, ?denied, "xmtp outbound rate limited (open)");
            let notice = self.cfg.notices.xmtp_rate_limited.clone();
            return self.notice(&from, notice, now);
        }
        let req = self.take_req();
        let mut pending = PendingOpen { req, queued: vec![text], attempts: 0 };
        let mut actions = Vec::new();
        if self.ready.is_some() {
            pending.attempts = 1;
            actions.push(Action::Agent(AgentRequest::OpenThread { req }));
        }
        tracing::info!(user = %from, req, "opening a thread for a new sender");
        self.open_reqs.insert(req, from.clone());
        self.pending_open.insert(from, pending);
        actions
    }

    fn on_agent_event(&mut self, event: AgentEvent, now: Instant) -> Vec<Action> {
        match event {
            AgentEvent::Ready { .. } => Vec::new(), // handled in on_agent
            AgentEvent::Opened { req, thread } => {
                let Some(user) = self.open_reqs.remove(&req) else { return Vec::new() };
                let Some(pending) = self.pending_open.remove(&user) else { return Vec::new() };
                for evicted in self.sessions.insert(user.clone(), thread.clone()) {
                    tracing::info!(user = %evicted, "session evicted (max_sessions reached)");
                }
                pending
                    .queued
                    .into_iter()
                    .flat_map(|text| self.queue_send(user.clone(), thread.clone(), text, now))
                    .collect()
            }
            AgentEvent::OpenFailed { req, error } => {
                let Some(user) = self.open_reqs.remove(&req) else { return Vec::new() };
                self.pending_open.remove(&user);
                tracing::warn!(user = %user, %error, "could not open a thread");
                let notice = self.cfg.notices.agent_unavailable.clone();
                self.notice(&user, notice, now)
            }
            AgentEvent::SendOk { req } => {
                self.inflight.remove(&req);
                Vec::new()
            }
            AgentEvent::SendFailed { req, error } => {
                let Some(send) = self.inflight.remove(&req) else { return Vec::new() };
                tracing::warn!(user = %send.user, %error, "send to agent failed");
                let notice = self.cfg.notices.agent_unavailable.clone();
                self.notice(&send.user, notice, now)
            }
            AgentEvent::Message { thread, id, sent_at_ns, text } => {
                if !self.seen_agent.insert(id.clone()) {
                    return Vec::new();
                }
                let Some(user) = self.sessions.user_for(&thread).cloned() else {
                    tracing::debug!(thread = %short(&thread), "message in an unmapped thread");
                    return Vec::new();
                };
                if self.sessions.cursor(&thread).is_some_and(|c| sent_at_ns <= c) {
                    return Vec::new(); // settled before a restart
                }
                if text.trim().is_empty() {
                    self.sessions.advance_cursor(&thread, sent_at_ns);
                    return Vec::new();
                }
                self.sessions.touch(&user);
                let ack = DeliveryAck { thread, message_id: id, sent_at_ns };
                vec![Action::Frontend(Outbound { to: user, text, ack: Some(ack) })]
            }
            AgentEvent::Fatal { error } => {
                tracing::error!(%error, "agent link reported a fatal error");
                Vec::new()
            }
        }
    }

    /// After a (re)connect: resume cursors, then re-send everything
    /// unacknowledged, in original order.
    fn replay(&mut self, now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();
        if !self.sessions.is_empty() {
            actions.push(Action::Agent(AgentRequest::Resume { threads: self.sessions.cursors() }));
        }
        let max = self.cfg.max_send_attempts;
        let mut gave_up = Vec::new();
        for (&req, user) in &self.open_reqs {
            let Some(pending) = self.pending_open.get_mut(user) else { continue };
            if pending.attempts >= max {
                gave_up.push(req);
                continue;
            }
            pending.attempts += 1;
            actions.push(Action::Agent(AgentRequest::OpenThread { req: pending.req }));
        }
        for req in gave_up {
            if let Some(user) = self.open_reqs.remove(&req) {
                self.pending_open.remove(&user);
                let notice = self.cfg.notices.agent_unavailable.clone();
                actions.extend(self.notice(&user, notice, now));
            }
        }
        let mut dropped = Vec::new();
        for (&req, send) in &mut self.inflight {
            if send.attempts >= max {
                dropped.push(req);
                continue;
            }
            send.attempts += 1;
            actions.push(Action::Agent(AgentRequest::Send {
                req,
                thread: send.thread.clone(),
                text: send.text.clone(),
            }));
        }
        for req in dropped {
            if let Some(send) = self.inflight.remove(&req) {
                let notice = self.cfg.notices.agent_unavailable.clone();
                actions.extend(self.notice(&send.user, notice, now));
            }
        }
        actions
    }

    fn queue_send(&mut self, user: UserAddress, thread: String, text: String, now: Instant) -> Vec<Action> {
        if let Err(denied) = self.xmtp_limiter.admit(&user, now) {
            tracing::info!(user = %user, ?denied, "xmtp outbound rate limited (send)");
            let notice = self.cfg.notices.xmtp_rate_limited.clone();
            return self.notice(&user, notice, now);
        }
        let req = self.take_req();
        let mut send = InflightSend { user, thread, text, attempts: 0 };
        let mut actions = Vec::new();
        if self.ready.is_some() {
            send.attempts = 1;
            actions.push(Action::Agent(AgentRequest::Send {
                req,
                thread: send.thread.clone(),
                text: send.text.clone(),
            }));
        }
        self.inflight.insert(req, send);
        actions
    }

    fn settle(&mut self, ack: &DeliveryAck) {
        self.sessions.advance_cursor(&ack.thread, ack.sent_at_ns);
    }

    fn notice(&mut self, to: &UserAddress, text: String, now: Instant) -> Vec<Action> {
        if self.notice_buckets.bucket(to, now).try_take(now) {
            vec![Action::Frontend(Outbound { to: to.clone(), text, ack: None })]
        } else {
            Vec::new()
        }
    }

    const fn take_req(&mut self) -> u64 {
        let req = self.next_req;
        self.next_req += 1;
        req
    }
}

/// First 8 characters of an id, for logs.
fn short(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn cfg() -> RouterConfig {
        RouterConfig {
            limits: InboundLimits {
                per_sender: Rate::new(3, 1, 10),
                global: Rate::new(100, 100, 1),
                new_senders: Rate::new(2, 1, 60),
                max_tracked_senders: 100,
            },
            xmtp_limits: XmtpOutboundLimits {
                per_user: Rate::new(100, 100, 1),
                global: Rate::new(100, 100, 1),
                max_tracked_users: 100,
            },
            max_sessions: 10,
            max_message_chars: 20,
            notice_cooldown_secs: 60,
            max_queued_while_opening: 2,
            max_send_attempts: 2,
            max_inflight: 3,
            dedupe_capacity: 100,
            hold_while_offline: true,
            notices: Notices::default(),
        }
    }

    fn user(id: &str) -> UserAddress {
        UserAddress::new("signal", id)
    }

    fn inbound(from: &str, id: &str, text: &str) -> FrontendEvent {
        FrontendEvent::Message(Inbound { from: user(from), id: id.into(), text: text.into() })
    }

    fn ev(event: AgentEvent) -> AgentInput {
        AgentInput::Event { generation: 1, event }
    }

    fn ready(generation: u64) -> AgentInput {
        AgentInput::Event {
            generation,
            event: AgentEvent::Ready { inbox_id: "own".into(), address: "0x1".into(), agent_inbox_id: "agent".into() },
        }
    }

    fn ready_router(t0: Instant) -> Router {
        let mut r = Router::new(cfg(), SessionStore::new(10), t0);
        assert!(r.on_agent(ready(1), t0).is_empty(), "nothing to replay");
        r
    }

    fn notices_to(actions: &[Action], to: &str) -> usize {
        actions.iter().filter(|a| matches!(a, Action::Frontend(o) if o.to == user(to) && o.ack.is_none())).count()
    }

    #[test]
    fn first_message_opens_a_thread_then_flows() {
        let t0 = Instant::now();
        let mut r = ready_router(t0);
        assert_eq!(
            r.on_frontend(inbound("a", "1", "hello"), t0),
            vec![Action::Agent(AgentRequest::OpenThread { req: 1 })]
        );
        // A second message while the thread opens is buffered, not sent.
        assert!(r.on_frontend(inbound("a", "2", "again"), t0).is_empty());
        let actions = r.on_agent(ev(AgentEvent::Opened { req: 1, thread: "t-a".into() }), t0);
        assert_eq!(
            actions,
            vec![
                Action::Agent(AgentRequest::Send { req: 2, thread: "t-a".into(), text: "hello".into() }),
                Action::Agent(AgentRequest::Send { req: 3, thread: "t-a".into(), text: "again".into() }),
            ]
        );
        // Later messages go straight to the thread.
        assert_eq!(
            r.on_frontend(inbound("a", "3", "third"), t0 + Duration::from_secs(10)),
            vec![Action::Agent(AgentRequest::Send { req: 4, thread: "t-a".into(), text: "third".into() })]
        );
    }

    #[test]
    fn agent_replies_reach_the_right_user_once() {
        let t0 = Instant::now();
        let mut r = ready_router(t0);
        r.on_frontend(inbound("a", "1", "hi"), t0);
        r.on_agent(ev(AgentEvent::Opened { req: 1, thread: "t-a".into() }), t0);
        let reply =
            AgentEvent::Message { thread: "t-a".into(), id: "m1".into(), sent_at_ns: 100, text: "hello a".into() };
        let actions = r.on_agent(ev(reply.clone()), t0);
        let ack = DeliveryAck { thread: "t-a".into(), message_id: "m1".into(), sent_at_ns: 100 };
        assert_eq!(
            actions,
            vec![Action::Frontend(Outbound { to: user("a"), text: "hello a".into(), ack: Some(ack.clone()) })]
        );
        assert!(r.on_agent(ev(reply), t0).is_empty(), "duplicate agent message dropped");
        r.on_frontend(FrontendEvent::Delivered(ack), t0);
        assert_eq!(r.sessions_mut().cursor("t-a"), Some(100));
        let stray = AgentEvent::Message { thread: "t-x".into(), id: "m2".into(), sent_at_ns: 1, text: "?".into() };
        assert!(r.on_agent(ev(stray), t0).is_empty(), "unmapped threads are ignored");
    }

    #[test]
    fn rate_limits_send_one_notice_per_cooldown() {
        let t0 = Instant::now();
        let mut r = ready_router(t0);
        r.on_frontend(inbound("a", "1", "x"), t0);
        r.on_agent(ev(AgentEvent::Opened { req: 1, thread: "t-a".into() }), t0);
        r.on_frontend(inbound("a", "2", "x"), t0);
        r.on_frontend(inbound("a", "3", "x"), t0);
        let denied = r.on_frontend(inbound("a", "4", "x"), t0);
        assert_eq!(notices_to(&denied, "a"), 1, "first denial is explained");
        let denied_again = r.on_frontend(inbound("a", "5", "x"), t0 + Duration::from_secs(1));
        assert!(denied_again.is_empty(), "no second notice inside the cooldown");
    }

    #[test]
    fn xmtp_outbound_rate_limit_denies_sends_and_opens() {
        let t0 = Instant::now();
        let mut cfg = cfg();
        // One call to XMTP per user, ever (within the window): enough to
        // open a thread, not enough to also send the first message.
        cfg.xmtp_limits = XmtpOutboundLimits {
            per_user: Rate::new(1, 1, 3600),
            global: Rate::new(100, 100, 1),
            max_tracked_users: 100,
        };
        let mut r = Router::new(cfg, SessionStore::new(10), t0);
        assert!(r.on_agent(ready(1), t0).is_empty(), "nothing to replay");
        // First message: opening the thread spends the user's one XMTP token.
        assert_eq!(
            r.on_frontend(inbound("a", "1", "hello"), t0),
            vec![Action::Agent(AgentRequest::OpenThread { req: 1 })]
        );
        // The thread opens and the buffered text tries to send, but the
        // user's XMTP bucket is now empty: denied, with a notice instead of
        // a send, and no further requests to the agent.
        let actions = r.on_agent(ev(AgentEvent::Opened { req: 1, thread: "t-a".into() }), t0);
        assert_eq!(notices_to(&actions, "a"), 1);
        assert!(!actions.iter().any(|a| matches!(a, Action::Agent(_))));
        // A different user has their own, untouched bucket.
        assert_eq!(
            r.on_frontend(inbound("b", "1", "hi"), t0),
            vec![Action::Agent(AgentRequest::OpenThread { req: 2 })]
        );
    }

    #[test]
    fn xmtp_global_outbound_rate_limit_applies_across_users() {
        let t0 = Instant::now();
        let mut cfg = cfg();
        cfg.xmtp_limits = XmtpOutboundLimits {
            per_user: Rate::new(100, 100, 1),
            global: Rate::new(1, 1, 3600),
            max_tracked_users: 100,
        };
        let mut r = Router::new(cfg, SessionStore::new(10), t0);
        assert!(r.on_agent(ready(1), t0).is_empty());
        // "a" opens a thread, spending the one global XMTP token.
        assert_eq!(
            r.on_frontend(inbound("a", "1", "hello"), t0),
            vec![Action::Agent(AgentRequest::OpenThread { req: 1 })]
        );
        // "b" is denied even though their own per-user bucket is untouched.
        let denied = r.on_frontend(inbound("b", "1", "hi"), t0);
        assert_eq!(notices_to(&denied, "b"), 1);
        assert!(!denied.iter().any(|a| matches!(a, Action::Agent(_))));
    }

    #[test]
    fn new_sender_budget_is_enforced() {
        let t0 = Instant::now();
        let mut r = ready_router(t0);
        assert_eq!(r.on_frontend(inbound("a", "1", "x"), t0).len(), 1);
        assert_eq!(r.on_frontend(inbound("b", "1", "x"), t0).len(), 1);
        let denied = r.on_frontend(inbound("c", "1", "x"), t0);
        assert_eq!(notices_to(&denied, "c"), 1);
        assert!(!denied.iter().any(|a| matches!(a, Action::Agent(_))), "no thread opened for c");
    }

    #[test]
    fn oversized_and_duplicate_messages_never_reach_the_agent() {
        let t0 = Instant::now();
        let mut r = ready_router(t0);
        let long = r.on_frontend(inbound("a", "1", &"x".repeat(21)), t0);
        assert_eq!(notices_to(&long, "a"), 1);
        assert!(
            long[0]
                == Action::Frontend(Outbound {
                    to: user("a"),
                    text: "That message is too long. Please keep it under 20 characters.".into(),
                    ack: None
                })
        );
        assert_eq!(r.on_frontend(inbound("a", "2", "ok"), t0).len(), 1);
        assert!(r.on_frontend(inbound("a", "2", "ok"), t0).is_empty(), "redelivered id dropped");
        assert!(r.on_frontend(inbound("a", "3", "   "), t0).is_empty(), "blank dropped");
    }

    #[test]
    fn opening_buffer_is_bounded() {
        let t0 = Instant::now();
        let mut r = ready_router(t0);
        r.on_frontend(inbound("a", "1", "one"), t0);
        r.on_frontend(inbound("a", "2", "two"), t0);
        let over = r.on_frontend(inbound("a", "3", "three"), t0);
        assert_eq!(notices_to(&over, "a"), 1);
    }

    #[test]
    fn requests_wait_for_ready_and_replay_after_restart() {
        let t0 = Instant::now();
        let mut r = Router::new(cfg(), SessionStore::new(10), t0);
        // Worker not ready yet: the open is recorded but not emitted, and the
        // user is told their message is held.
        let held = r.on_frontend(inbound("a", "1", "hi"), t0);
        assert_eq!(notices_to(&held, "a"), 1);
        assert!(!held.iter().any(|a| matches!(a, Action::Agent(_))));
        assert_eq!(r.on_agent(ready(1), t0), vec![Action::Agent(AgentRequest::OpenThread { req: 1 })]);
        r.on_agent(ev(AgentEvent::Opened { req: 1, thread: "t-a".into() }), t0);
        // Send 2 is in flight when the worker dies.
        r.on_agent(AgentInput::Down { generation: 1 }, t0);
        assert_eq!(r.agent_generation(), None);
        // A late event from the dead worker is ignored.
        assert!(r.on_agent(ev(AgentEvent::SendOk { req: 2 }), t0).is_empty());
        let replay = r.on_agent(ready(2), t0);
        assert_eq!(
            replay,
            vec![
                Action::Agent(AgentRequest::Resume {
                    threads: vec![crate::protocol::ResumeCursor { thread: "t-a".into(), after_ns: 0 }]
                }),
                Action::Agent(AgentRequest::Send { req: 2, thread: "t-a".into(), text: "hi".into() }),
            ]
        );
        // Attempts are capped: the next restart gives up and tells the user
        // (after the notice cooldown from the earlier "offline" notice).
        r.on_agent(AgentInput::Down { generation: 2 }, t0);
        let gave_up = r.on_agent(ready(3), t0 + Duration::from_secs(61));
        assert_eq!(notices_to(&gave_up, "a"), 1);
        assert!(!gave_up.iter().any(|a| matches!(a, Action::Agent(AgentRequest::Send { .. }))));
    }

    #[test]
    fn offline_messages_are_dropped_when_not_held() {
        let t0 = Instant::now();
        let mut cfg = cfg();
        cfg.hold_while_offline = false;
        let mut r = Router::new(cfg, SessionStore::new(10), t0);
        // Worker not ready: the message is dropped (one notice), nothing is
        // recorded for later.
        let dropped = r.on_frontend(inbound("a", "1", "hi"), t0);
        assert_eq!(notices_to(&dropped, "a"), 1);
        assert!(!dropped.iter().any(|a| matches!(a, Action::Agent(_))));
        assert!(r.on_agent(ready(1), t0).is_empty(), "nothing was held for replay");
        // Once the link is up, messages flow normally.
        assert_eq!(
            r.on_frontend(inbound("a", "2", "hi again"), t0),
            vec![Action::Agent(AgentRequest::OpenThread { req: 1 })]
        );
    }

    #[test]
    fn undelivered_backlog_is_bounded() {
        let t0 = Instant::now();
        let mut r = ready_router(t0);
        r.on_frontend(inbound("a", "1", "one"), t0);
        r.on_agent(ev(AgentEvent::Opened { req: 1, thread: "t-a".into() }), t0);
        r.on_frontend(inbound("a", "2", "two"), t0);
        r.on_frontend(inbound("a", "3", "three"), t0);
        // Three sends are unacknowledged (max_inflight = 3).
        let over = r.on_frontend(inbound("a", "4", "four"), t0 + Duration::from_secs(10));
        assert_eq!(notices_to(&over, "a"), 1);
        assert!(!over.iter().any(|a| matches!(a, Action::Agent(_))));
    }

    #[test]
    fn send_failure_notifies_the_user() {
        let t0 = Instant::now();
        let mut r = ready_router(t0);
        r.on_frontend(inbound("a", "1", "hi"), t0);
        r.on_agent(ev(AgentEvent::Opened { req: 1, thread: "t-a".into() }), t0);
        let failed = r.on_agent(ev(AgentEvent::SendFailed { req: 2, error: "boom".into() }), t0);
        assert_eq!(notices_to(&failed, "a"), 1);
    }

    #[test]
    fn settled_messages_are_not_redelivered_after_restart() {
        let t0 = Instant::now();
        let mut sessions = SessionStore::new(10);
        sessions.insert(user("a"), "t-a".into());
        sessions.advance_cursor("t-a", 500);
        let mut r = Router::new(cfg(), sessions, t0);
        r.on_agent(ready(1), t0);
        let old = AgentEvent::Message { thread: "t-a".into(), id: "old".into(), sent_at_ns: 500, text: "old".into() };
        assert!(r.on_agent(ev(old), t0).is_empty());
        let new = AgentEvent::Message { thread: "t-a".into(), id: "new".into(), sent_at_ns: 501, text: "new".into() };
        assert_eq!(r.on_agent(ev(new), t0).len(), 1);
    }
}

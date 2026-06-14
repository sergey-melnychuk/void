//! In-process room state. Everything lives in memory — no database, no Redis.
//! A [`DashMap`] holds every live room; each [`Room`] owns a broadcast channel
//! for fan-out and a `Mutex`-guarded [`RoomInner`] for mutable content.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::config::Config;

pub type Rooms = DashMap<String, Arc<Room>>;

pub struct AppState {
    pub rooms: Rooms,
    pub config: Config,
}

impl AppState {
    pub fn new(config: Config) -> Arc<Self> {
        Arc::new(AppState { rooms: DashMap::new(), config })
    }
}

/// Milliseconds since the Unix epoch. Never panics: a clock set before the
/// epoch (misconfigured host) yields 0 rather than taking down the task.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone, Serialize)]
pub struct Message {
    pub id: u64,
    pub role: String, // "admin" | "user"
    pub text: String,
    pub ts: u64,
    /// emoji -> count
    pub reactions: BTreeMap<String, u32>,
}

#[derive(Clone, Serialize)]
pub struct Question {
    pub id: u64,
    pub text: String,
    pub votes: u32,
    pub pinned: bool,
    pub answered: bool,
}

#[derive(Clone, Serialize)]
pub struct PollOption {
    pub text: String,
    pub votes: u32,
}

#[derive(Clone, Serialize)]
pub struct Poll {
    pub id: u64,
    pub question: String,
    pub options: Vec<PollOption>,
    pub closed: bool,
}

/// Per-room configuration captured at creation (already clamped to limits).
pub struct RoomConfig {
    pub max_messages: usize,
    pub max_participants: usize,
    pub rate_limit_ms: u64,
    pub max_message_length: usize,
    pub moderated: bool,
}

pub struct Room {
    pub id: String,
    pub title: Option<String>,
    /// Admin secret — kept so the admin token can be recomputed and compared.
    pub secret: String,
    pub password_hash: Option<String>,
    pub expires_at: u64, // epoch ms
    pub cfg: RoomConfig,
    /// Broadcast channel: serialized server→client event JSON strings.
    pub tx: broadcast::Sender<String>,
    /// Admin-only channel: pending item notifications, invisible to regular users.
    pub admin_tx: broadcast::Sender<String>,
    pub inner: Mutex<RoomInner>,
}

#[derive(Default)]
pub struct RoomInner {
    pub messages: VecDeque<Message>,
    pub questions: Vec<Question>,
    pub polls: Vec<Poll>,

    pub pending_messages: Vec<Message>,
    pub pending_questions: Vec<Question>,

    pub locked: bool,
    pub locked_until: Option<u64>, // epoch ms, for timed locks
    /// Monotonic counter bumped on every lock change; a timed-unlock task only
    /// fires if the epoch it captured is still current (so a later manual
    /// lock/unlock is never clobbered by a stale timer).
    pub lock_epoch: u64,
    pub participants: usize,

    next_msg_id: u64,
    next_question_id: u64,
    next_poll_id: u64,

    /// Session/identity tokens admitted to this room (enforces the password
    /// gate at the WebSocket layer and supports cookie reconnect within TTL).
    pub sessions: HashSet<String>,

    /// identity -> last message timestamp (ms), for per-user rate limiting.
    pub last_message_at: HashMap<String, u64>,

    /// dedup sets — one action per identity.
    pub question_voters: HashMap<u64, HashSet<String>>,
    pub poll_voters: HashMap<u64, HashSet<String>>,
    /// (message_id, emoji) -> identities who reacted (for toggle semantics).
    pub reaction_users: HashMap<(u64, String), HashSet<String>>,
}

impl Room {
    pub fn new(
        id: String,
        title: Option<String>,
        secret: String,
        password_hash: Option<String>,
        ttl_seconds: u64,
        cfg: RoomConfig,
    ) -> Arc<Room> {
        // Generous buffer so a briefly-slow client at the 200-user target does
        // not lag out of the ring (and if one does, it resyncs — see ws.rs).
        let (tx, _rx) = broadcast::channel(1024);
        let (admin_tx, _arx) = broadcast::channel(256);
        Arc::new(Room {
            id,
            title,
            secret,
            password_hash,
            expires_at: now_ms().saturating_add(ttl_seconds.saturating_mul(1000)),
            cfg,
            tx,
            admin_tx,
            inner: Mutex::new(RoomInner::default()),
        })
    }

    pub fn is_expired(&self) -> bool {
        now_ms() >= self.expires_at
    }

    /// Does `token` match this room's admin token? Recomputed each call (the
    /// admin token is never stored, per the security model) and compared in
    /// constant time. Centralizes the check shared by the WS and state handlers.
    pub fn is_admin_token(&self, token: &str) -> bool {
        crate::auth::ct_eq(token, &crate::auth::admin_token(&self.secret, &self.id))
    }

    /// Broadcast a server event to every subscriber. Errors (no receivers)
    /// are ignored — an empty room is fine.
    pub fn broadcast(&self, event: Value) {
        let _ = self.tx.send(event.to_string());
    }

    /// Broadcast an event only to admin connections.
    pub fn broadcast_admin(&self, event: Value) {
        let _ = self.admin_tx.send(event.to_string());
    }

    /// Lock the room, run `f`, release the lock, then broadcast whatever event
    /// `f` returns (if any). Keeps the lock-mutate-broadcast pattern in one
    /// place and guarantees the mutex is never held across the send.
    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut RoomInner) -> Option<Value>,
    {
        let event = {
            let mut inner = self.inner.lock().unwrap();
            f(&mut inner)
        };
        if let Some(event) = event {
            self.broadcast(event);
        }
    }

    /// Build the admin state snapshot (returned by `GET /r/:id/state`).
    pub fn snapshot(&self) -> Value {
        self.inner.lock().unwrap().snapshot_for(&self.id, true, self.title.as_deref())
    }
}

impl RoomInner {
    /// Effective lock state: a timed lock whose deadline has passed reads as
    /// unlocked even if the auto-unlock task hasn't fired yet (self-healing).
    pub fn effective_locked(&self) -> bool {
        self.locked && self.locked_until.map_or(true, |until| now_ms() < until)
    }

    pub fn snapshot(&self, room_id: &str, title: Option<&str>) -> Value {
        self.snapshot_for(room_id, false, title)
    }

    pub fn snapshot_for(&self, room_id: &str, is_admin: bool, title: Option<&str>) -> Value {
        let locked = self.effective_locked();
        let mut v = json!({
            "id": room_id,
            "title": title,
            "locked": locked,
            "locked_until": if locked { self.locked_until } else { None },
            "participants": self.participants,
            "messages": self.messages.iter().collect::<Vec<_>>(),
            "questions": self.questions,
            "polls": self.polls,
        });
        if is_admin {
            v["pending_messages"] = json!(self.pending_messages);
            v["pending_questions"] = json!(self.pending_questions);
        }
        v
    }

    pub fn next_msg_id(&mut self) -> u64 {
        self.next_msg_id += 1;
        self.next_msg_id
    }
    pub fn next_question_id(&mut self) -> u64 {
        self.next_question_id += 1;
        self.next_question_id
    }
    pub fn next_poll_id(&mut self) -> u64 {
        self.next_poll_id += 1;
        self.next_poll_id
    }
}

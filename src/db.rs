//! Postgres persistence layer.
//!
//! All writes are fire-and-forget (caller spawns): a transient DB hiccup
//! never stalls a WebSocket message. On startup, `replay_all` reconstructs
//! in-memory state from the event log.

use std::sync::Arc;

use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use crate::config::Config;
use crate::state::{Room, RoomConfig, now_ms};

// ── Pool & migrations ─────────────────────────────────────────────────────

pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPool::connect(url).await
}

pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./db").run(pool).await
}

// ── Writes ────────────────────────────────────────────────────────────────

/// Insert a room row. Uses ON CONFLICT DO NOTHING so replayed startup rooms
/// (e.g. the test room if we ever persist it) are idempotent.
pub async fn persist_room(pool: &PgPool, room: &Room) {
    // Store NULL for never-expiring rooms instead of saturating u64 → i64.
    let expires_at: Option<i64> = if room.expires_at == u64::MAX {
        None
    } else {
        Some(room.expires_at as i64)
    };
    if let Err(e) = sqlx::query(
        "INSERT INTO rooms
         (id, title, secret, password_hash, expires_at,
          max_messages, max_participants, rate_limit_ms, max_message_length, moderated)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(&room.id)
    .bind(room.title.as_deref())
    .bind(&room.secret)
    .bind(room.password_hash.as_deref())
    .bind(expires_at)
    .bind(room.cfg.max_messages as i32)
    .bind(room.cfg.max_participants as i32)
    .bind(room.cfg.rate_limit_ms as i64)
    .bind(room.cfg.max_message_length as i32)
    .bind(room.cfg.moderated)
    .execute(pool)
    .await
    {
        eprintln!("db: persist_room [{}] failed: {e}", room.id);
    }
}

/// Append one event row. payload is a JSON-encoded string stored as TEXT.
pub async fn append_event(pool: &PgPool, room_id: &str, kind: &str, payload: Value) {
    let ts = now_ms() as i64;
    if let Err(e) =
        sqlx::query("INSERT INTO events (room_id, ts, kind, payload) VALUES ($1,$2,$3,$4)")
            .bind(room_id)
            .bind(ts)
            .bind(kind)
            .bind(payload.to_string())
            .execute(pool)
            .await
    {
        eprintln!("db: append_event [{kind}@{room_id}] failed: {e}");
    }
}

pub async fn delete_room(pool: &PgPool, room_id: &str) {
    if let Err(e) = sqlx::query("DELETE FROM rooms WHERE id = $1")
        .bind(room_id)
        .execute(pool)
        .await
    {
        eprintln!("db: delete_room [{room_id}] failed: {e}");
    }
}

/// Delete rooms whose expires_at deadline has passed (sweep companion).
pub async fn delete_expired(pool: &PgPool) {
    let now = now_ms() as i64;
    if let Err(e) =
        sqlx::query("DELETE FROM rooms WHERE expires_at IS NOT NULL AND expires_at <= $1")
            .bind(now)
            .execute(pool)
            .await
    {
        eprintln!("db: delete_expired failed: {e}");
    }
}

// ── Startup replay ────────────────────────────────────────────────────────

/// Load every non-expired room from the DB and replay its event log into
/// in-memory state. Returns the live rooms ready to be inserted into AppState.
pub async fn replay_all(pool: &PgPool, _config: &Config) -> Vec<Arc<Room>> {
    let now = now_ms();

    let room_rows = match sqlx::query(
        "SELECT id, title, secret, password_hash, expires_at,
                max_messages, max_participants, rate_limit_ms, max_message_length, moderated
         FROM rooms",
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("db: replay_all load rooms failed: {e}");
            return vec![];
        }
    };

    let mut rooms = Vec::new();

    for row in room_rows {
        let id: String = row.get("id");

        // NULL → u64::MAX (never expires).
        let expires_at: Option<i64> = row.get("expires_at");
        let expires_at_u64 = expires_at.map(|v| v as u64).unwrap_or(u64::MAX);

        if expires_at_u64 != u64::MAX && now >= expires_at_u64 {
            continue; // expired while we were down; sweep will clean it up
        }

        let cfg = RoomConfig {
            max_messages: row.get::<i32, _>("max_messages") as usize,
            max_participants: row.get::<i32, _>("max_participants") as usize,
            rate_limit_ms: row.get::<i64, _>("rate_limit_ms") as u64,
            max_message_length: row.get::<i32, _>("max_message_length") as usize,
            moderated: row.get("moderated"),
        };

        let room = Room::new(
            id.clone(),
            row.get("title"),
            row.get("secret"),
            row.get("password_hash"),
            expires_at_u64,
            cfg,
        );

        // Replay event log in insert order.
        let event_rows =
            match sqlx::query("SELECT kind, payload FROM events WHERE room_id = $1 ORDER BY id")
                .bind(&id)
                .fetch_all(pool)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("db: replay events for {id} failed: {e}");
                    continue;
                }
            };

        {
            let mut inner = room.inner.lock().unwrap();
            for erow in event_rows {
                let kind: String = erow.get("kind");
                let raw: String = erow.get("payload");
                let payload: Value = serde_json::from_str(&raw).unwrap_or(json!({}));
                inner.apply_event(&kind, &payload);
            }
        }

        rooms.push(room);
    }

    rooms
}

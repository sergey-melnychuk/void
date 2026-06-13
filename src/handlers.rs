//! REST surface: room creation, join, state snapshot, and the background TTL
//! sweep. All real-time interaction happens over WebSocket (see `ws.rs`).
//!
//! There are no cookies: the session token is minted by the server, returned
//! in the join response, and persisted client-side in `localStorage` (the
//! frontend needs it readable to put it in the WebSocket subprotocol). So no
//! cookie-consent concern, and the server stays the admission authority.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;

use crate::auth;
use crate::state::{AppState, Room, RoomConfig};

// ── POST /rooms ────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct CreateRoomReq {
    pub ttl_seconds: Option<u64>,
    pub password: Option<String>,
    pub max_participants: Option<usize>,
    pub max_messages: Option<usize>,
    pub rate_limit_seconds: Option<u64>,
    pub moderated: Option<bool>,
}

pub async fn create_room(
    State(app): State<Arc<AppState>>,
    body: Option<Json<CreateRoomReq>>,
) -> Response {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let cfg = &app.config;

    let ttl = cfg.clamp_ttl(req.ttl_seconds);
    let room_cfg = RoomConfig {
        max_messages: cfg.clamp_max_messages(req.max_messages),
        max_participants: cfg.clamp_max_participants(req.max_participants),
        rate_limit_ms: cfg.clamp_rate_limit(req.rate_limit_seconds) * 1000,
        max_message_length: cfg.max_message_length,
        moderated: req.moderated.unwrap_or(false),
    };

    let password_hash = match req.password.as_deref().filter(|p| !p.is_empty()) {
        Some(pw) => match auth::hash_password(pw, cfg.bcrypt_cost) {
            Ok(h) => Some(h),
            Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "hash failed").into_response(),
        },
        None => None,
    };

    // Find a free room ID (retry on the unlikely collision).
    let mut room_id = auth::new_room_id();
    for _ in 0..8 {
        if !app.rooms.contains_key(&room_id) {
            break;
        }
        room_id = auth::new_room_id();
    }
    if app.rooms.contains_key(&room_id) {
        return (StatusCode::INTERNAL_SERVER_ERROR, "could not allocate room id").into_response();
    }

    let secret = auth::new_secret();
    let room = Room::new(room_id.clone(), secret.clone(), password_hash, ttl, room_cfg);
    app.rooms.insert(room_id.clone(), room);

    let base = cfg.base_url.trim_end_matches('/');
    Json(json!({
        "room_id": room_id,
        "public_url": format!("{base}/r/{room_id}"),
        "admin_url": format!("{base}/r/{room_id}/{secret}"),
    }))
    .into_response()
}

// ── POST /r/:id/join ────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct JoinReq {
    pub password: Option<String>,
    /// An existing session token (from localStorage) for reconnect.
    pub session: Option<String>,
}

pub async fn join_room(
    State(app): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    body: Option<Json<JoinReq>>,
) -> Response {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let Some(room) = app.rooms.get(&room_id).map(|r| r.clone()) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    if room.is_expired() {
        return (StatusCode::NOT_FOUND, "room expired").into_response();
    }

    // A returning session that's still admitted skips the password check and
    // the participant cap — that's the reconnect path.
    let reconnecting = req
        .session
        .as_ref()
        .map(|s| room.inner.lock().unwrap().sessions.contains(s))
        .unwrap_or(false);

    if !reconnecting {
        if let Some(hash) = &room.password_hash {
            let ok = req
                .password
                .as_deref()
                .map(|pw| auth::verify_password(pw, hash))
                .unwrap_or(false);
            if !ok {
                return (StatusCode::FORBIDDEN, "bad or missing password").into_response();
            }
        }
    }

    // Concurrent room size is capped at the WebSocket layer (live connections);
    // we deliberately do NOT cap the lifetime session set here. Without accounts
    // or per-IP limits (which the spec rules out), a client can always mint a
    // fresh identity by re-joining, so a join-time cap would only lock out
    // legitimate reconnects from a cleared browser while not stopping abuse.
    let (token, snapshot) = {
        let mut inner = room.inner.lock().unwrap();
        let token = match (reconnecting, &req.session) {
            (true, Some(t)) => t.clone(),
            _ => auth::new_session_token(),
        };
        inner.sessions.insert(token.clone());
        (token, inner.snapshot(&room.id))
    };

    Json(json!({ "session_token": token, "state": snapshot })).into_response()
}

// ── GET /r/:id/state ─────────────────────────────────────────────────────────

/// Snapshot endpoint for the admin (pre-connect check + initial state). Regular
/// participants get their snapshot as the first WebSocket frame instead, so
/// this requires the admin token, supplied via `Authorization: Bearer <token>`
/// — never a query param, so it stays out of access logs.
pub async fn room_state(
    State(app): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(room) = app.rooms.get(&room_id).map(|r| r.clone()) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    if room.is_expired() {
        return (StatusCode::NOT_FOUND, "room expired").into_response();
    }

    let authorized = bearer(&headers).map(|t| room.is_admin_token(&t)).unwrap_or(false);
    if !authorized {
        return (StatusCode::FORBIDDEN, "admin token required").into_response();
    }
    Json(room.snapshot()).into_response()
}

/// Extract a bearer token from the `Authorization` header. The scheme is
/// matched case-insensitively (per RFC 7235) with surrounding whitespace
/// tolerated.
fn bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?.trim();
    let (scheme, token) = raw.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim().to_string())
}

// ── TTL sweep ────────────────────────────────────────────────────────────────

/// Background task: periodically reap expired rooms so none are orphaned.
pub async fn sweep_expired(app: Arc<AppState>) {
    let mut tick = tokio::time::interval(Duration::from_secs(app.config.ttl_sweep_interval_seconds));
    loop {
        tick.tick().await;
        let expired: Vec<String> = app
            .rooms
            .iter()
            .filter(|e| e.value().is_expired())
            .map(|e| e.key().clone())
            .collect();
        for id in expired {
            if let Some((_, room)) = app.rooms.remove(&id) {
                room.broadcast(json!({ "type": "room_closed", "payload": {} }));
            }
        }
    }
}

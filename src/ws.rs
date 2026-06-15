//! WebSocket endpoint and the full client→server / server→client protocol
//! (see the WebSocket Protocol section of VOID.md).
//!
//! Each connection subscribes to the room's broadcast channel for fan-out and
//! owns a private mpsc channel for direct replies (errors, rate-limit notices,
//! the initial snapshot, lag resyncs). The session/admin token is carried in
//! the `Sec-WebSocket-Protocol` header rather than the URL, so it never lands
//! in access logs. Privileged actions require the admin token, recomputed from
//! the room secret and compared in constant time on connect.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{
        Path, State,
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};

use crate::db;
use crate::state::{AppState, Message, Poll, PollOption, Question, Room, now_ms};

const REACTIONS: [&str; 5] = ["👍", "❤️", "😂", "🔥", "🤔"];

/// Fire-and-forget DB event append. Skips the test room and no-DB configs.
fn persist(app: &Arc<AppState>, room_id: &str, kind: &'static str, payload: Value) {
    if room_id == "test" {
        return;
    }
    let Some(pool) = app.db.as_ref() else { return };
    let pool = pool.clone();
    let room_id = room_id.to_string();
    tokio::spawn(async move {
        db::append_event(&pool, &room_id, kind, payload).await;
    });
}
/// Subprotocol prefix carrying the auth token, e.g. `void.token.<token>`.
const TOKEN_PROTO_PREFIX: &str = "void.token.";

pub async fn ws_handler(
    ws: WebSocketUpgrade,
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

    // Token rides in the WebSocket subprotocol, not the query string.
    let offered = token_protocol(&headers);
    let token = offered.as_ref().map(|(_, t)| t.clone()).unwrap_or_default();
    let is_admin = room.is_admin_token(&token);

    // Non-admins must present a session token already admitted via POST /join.
    if !is_admin && !room.inner.lock().unwrap().sessions.contains(&token) {
        return (StatusCode::FORBIDDEN, "join required").into_response();
    }

    // Each admin connection gets a distinct identity, so two admin browsers
    // (a supported scenario) vote/react/rate-limit independently. Privileges
    // are gated on `is_admin`, not on the identity string.
    let is_display = !is_admin && token.starts_with("disp.");
    let identity = if is_admin {
        format!("admin:{}", room.secret)
    } else {
        token
    };

    // Echo the offered subprotocol so the browser handshake completes.
    let ws = match offered {
        Some((proto, _)) => ws.protocols([proto]),
        None => ws,
    };
    ws.on_upgrade(move |socket| handle_socket(socket, app, room, identity, is_admin, is_display))
}

/// Extract `(full_protocol, token)` from the `Sec-WebSocket-Protocol` header.
fn token_protocol(headers: &HeaderMap) -> Option<(String, String)> {
    let raw = headers.get(header::SEC_WEBSOCKET_PROTOCOL)?.to_str().ok()?;
    raw.split(',').map(str::trim).find_map(|p| {
        p.strip_prefix(TOKEN_PROTO_PREFIX)
            .map(|t| (p.to_string(), t.to_string()))
    })
}

async fn handle_socket(
    socket: WebSocket,
    app: Arc<AppState>,
    room: Arc<Room>,
    identity: String,
    is_admin: bool,
    is_display: bool,
) {
    // Subscribe BEFORE snapshotting so no event broadcast in the gap is lost;
    // any overlap is deduped by id on the client.
    let mut rx = room.tx.subscribe();

    // Enter: participant cap check + increment + snapshot, all under one lock.
    // Display connections are invisible — they don't count toward the cap or the counter.
    let (snapshot, count) = {
        let mut inner = room.inner.lock().unwrap();
        if !is_display {
            if inner.participants >= room.cfg.max_participants {
                return; // socket dropped → connection closed
            }
            inner.participants += 1;
        }
        (
            inner.snapshot_for(&room.id, is_admin, room.title.as_deref()),
            inner.participants,
        )
    };

    let (mut sink, mut stream) = socket.split();

    // The snapshot is the very first frame, sent before the broadcast pump
    // starts — so no live event can ever be delivered ahead of it.
    let first = json!({ "type": "snapshot", "payload": snapshot }).to_string();
    if sink.send(WsMessage::Text(first.into())).await.is_err() {
        if !is_display {
            leave(&room);
        }
        return;
    }
    if !is_display {
        broadcast_participants(&room, count);
    }

    let (ptx, mut prx) = mpsc::unbounded_channel::<String>();

    // Admin connections receive pending-item notifications via admin_tx.
    // Bridge admin_tx → ptx so the existing outbound pump handles delivery.
    if is_admin {
        let mut admin_rx = room.admin_tx.subscribe();
        let ptx2 = ptx.clone();
        tokio::spawn(async move {
            while let Ok(msg) = admin_rx.recv().await {
                if ptx2.send(msg).is_err() {
                    break;
                }
            }
        });
    }

    // Outbound pump: room broadcasts + this connection's private messages.
    let pump_room = room.clone();
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                r = rx.recv() => match r {
                    Ok(m) => if sink.send(WsMessage::Text(m.into())).await.is_err() { break },
                    // Fell behind the ring buffer. Drain everything still
                    // buffered, THEN send a fresh snapshot — so no stale
                    // buffered event lands on top of the newer snapshot.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        loop {
                            match rx.try_recv() {
                                Ok(_) => continue,
                                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                                Err(_) => break, // Empty or Closed
                            }
                        }
                        let snap = json!({ "type": "snapshot", "payload": pump_room.inner.lock().unwrap().snapshot_for(&pump_room.id, is_admin, pump_room.title.as_deref()) }).to_string();
                        if sink.send(WsMessage::Text(snap.into())).await.is_err() { break }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                Some(m) = prx.recv() => {
                    if sink.send(WsMessage::Text(m.into())).await.is_err() { break }
                }
            }
        }
    });

    // Inbound loop.
    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            WsMessage::Text(t) => handle_client(
                &app,
                &room,
                &identity,
                is_admin,
                is_display,
                t.as_str(),
                &ptx,
            ),
            WsMessage::Close(_) => break,
            _ => {}
        }
    }

    send_task.abort();
    if !is_display {
        leave(&room);
    }
}

/// Decrement the participant count and announce the new total.
fn leave(room: &Arc<Room>) {
    let count = {
        let mut inner = room.inner.lock().unwrap();
        inner.participants = inner.participants.saturating_sub(1);
        inner.participants
    };
    broadcast_participants(room, count);
}

fn broadcast_participants(room: &Room, count: usize) {
    room.broadcast(json!({ "type": "participant_count", "payload": { "count": count } }));
}

fn personal_err(ptx: &mpsc::UnboundedSender<String>, code: &str, message: &str) {
    let ev = json!({ "type": "error", "payload": { "code": code, "message": message } });
    let _ = ptx.send(ev.to_string());
}

fn role_str(is_admin: bool) -> String {
    if is_admin {
        "admin".into()
    } else {
        "user".into()
    }
}

/// Dispatch one inbound client message. Synchronous: it only mutates room
/// state and pushes onto channels (no awaits while holding the lock).
fn handle_client(
    app: &Arc<AppState>,
    room: &Arc<Room>,
    identity: &str,
    is_admin: bool,
    is_display: bool,
    text: &str,
    ptx: &mpsc::UnboundedSender<String>,
) {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return personal_err(ptx, "bad_json", "invalid JSON");
    };
    let kind = v.get("type").and_then(Value::as_str).unwrap_or("");
    let p = v.get("payload").cloned().unwrap_or_else(|| json!({}));

    // Display connections are read-only — silently drop all input.
    if is_display {
        return;
    }

    // Admin-only actions gate up front.
    if kind.starts_with("admin_") && !is_admin {
        return personal_err(ptx, "forbidden", "admin token required");
    }

    match kind {
        // ── user actions ────────────────────────────────────────────────
        "message" => {
            let text = p
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            if text.is_empty() {
                return;
            }
            let now = now_ms();
            let event = {
                let mut inner = room.inner.lock().unwrap();
                if inner.effective_locked() && !is_admin {
                    return personal_err(ptx, "locked", "room is locked");
                }
                if text.chars().count() > room.cfg.max_message_length {
                    return personal_err(ptx, "too_long", "message exceeds max length");
                }
                if !is_admin {
                    if let Some(&last) = inner.last_message_at.get(identity)
                        && now.saturating_sub(last) < room.cfg.rate_limit_ms {
                            return personal_err(ptx, "rate_limited", "slow down");
                        }
                    inner.last_message_at.insert(identity.to_string(), now);
                }
                let id = inner.next_msg_id();
                let msg = Message {
                    id,
                    role: role_str(is_admin),
                    text,
                    ts: now,
                    reactions: BTreeMap::new(),
                };
                if room.cfg.moderated && !is_admin {
                    inner.pending_messages.push(msg.clone());
                    drop(inner);
                    let ev = json!({ "type": "pending_message", "payload": msg });
                    let _ = ptx.send(ev.to_string()); // submitter sees their own pending item
                    room.broadcast_admin(ev); // admins see it in their queue
                    return;
                }
                inner.messages.push_back(msg.clone());
                while inner.messages.len() > room.cfg.max_messages {
                    inner.messages.pop_front();
                }
                json!({ "type": "message", "payload": msg })
            };
            if let Some(p) = event.get("payload") {
                persist(
                    app,
                    &room.id,
                    "message",
                    json!({"id": p["id"], "role": p["role"], "text": p["text"], "ts": p["ts"]}),
                );
            }
            room.broadcast(event);
        }

        "reaction" => {
            let (Some(mid), Some(emoji)) = (
                p.get("message_id").and_then(Value::as_u64),
                p.get("emoji").and_then(Value::as_str).map(str::to_string),
            ) else {
                return;
            };
            if !REACTIONS.contains(&emoji.as_str()) {
                return personal_err(ptx, "bad_emoji", "unsupported reaction");
            }
            let (event, added) = {
                let mut inner = room.inner.lock().unwrap();
                // Confirm the message still exists BEFORE touching dedup state,
                // so a reaction racing a delete leaves no orphaned entry.
                if !inner.messages.iter().any(|m| m.id == mid) {
                    return;
                }
                let users = inner
                    .reaction_users
                    .entry((mid, emoji.clone()))
                    .or_default();
                let added = if !users.remove(identity) {
                    users.insert(identity.to_string());
                    true
                } else {
                    false
                };
                let count = users.len() as u32;
                if count == 0 {
                    inner.reaction_users.remove(&(mid, emoji.clone())); // don't leak empty sets
                }
                let msg = inner.messages.iter_mut().find(|m| m.id == mid).unwrap();
                if count == 0 {
                    msg.reactions.remove(&emoji);
                } else {
                    msg.reactions.insert(emoji.clone(), count);
                }
                (
                    json!({ "type": "reaction", "payload": { "message_id": mid, "emoji": emoji, "count": count } }),
                    added,
                )
            };
            let kind = if added {
                "reaction_add"
            } else {
                "reaction_remove"
            };
            persist(
                app,
                &room.id,
                kind,
                json!({"message_id": mid, "emoji": emoji, "voter": identity}),
            );
            room.broadcast(event);
        }

        "question" => {
            let text = p
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            if text.is_empty() {
                return;
            }
            if text.chars().count() > room.cfg.max_message_length {
                return personal_err(ptx, "too_long", "question exceeds max length");
            }
            let event = {
                let mut inner = room.inner.lock().unwrap();
                if inner.effective_locked() && !is_admin {
                    return personal_err(ptx, "locked", "room is locked");
                }
                let id = inner.next_question_id();
                let q = Question {
                    id,
                    text,
                    votes: 0,
                    pinned: false,
                    answered: false,
                };
                if room.cfg.moderated && !is_admin {
                    inner.pending_questions.push(q.clone());
                    drop(inner);
                    let ev = json!({ "type": "pending_question", "payload": q });
                    let _ = ptx.send(ev.to_string());
                    room.broadcast_admin(ev);
                    return;
                }
                inner.questions.push(q.clone());
                json!({ "type": "question", "payload": q })
            };
            if let Some(p) = event.get("payload") {
                persist(
                    app,
                    &room.id,
                    "question",
                    json!({"id": p["id"], "text": p["text"]}),
                );
            }
            room.broadcast(event);
        }

        "vote" => {
            let Some(qid) = p.get("question_id").and_then(Value::as_u64) else {
                return;
            };
            let (event, removed) = {
                let mut inner = room.inner.lock().unwrap();
                if inner.effective_locked() && !is_admin {
                    return personal_err(ptx, "locked", "room is locked");
                }
                match inner.questions.iter().find(|q| q.id == qid) {
                    None => return,
                    Some(q) if q.answered => {
                        return personal_err(ptx, "answered", "question already answered");
                    }
                    _ => {}
                }
                let voters = inner.question_voters.entry(qid).or_default();
                let already = !voters.insert(identity.to_string());
                if already {
                    voters.remove(identity);
                }
                let q = inner.questions.iter_mut().find(|q| q.id == qid).unwrap();
                if already {
                    q.votes = q.votes.saturating_sub(1);
                } else {
                    q.votes += 1;
                }
                (
                    json!({ "type": "vote", "payload": { "question_id": qid, "votes": q.votes } }),
                    already,
                )
            };
            let kind = if removed { "vote_remove" } else { "vote_add" };
            persist(
                app,
                &room.id,
                kind,
                json!({"question_id": qid, "voter": identity}),
            );
            room.broadcast(event);
        }

        "poll_vote" => {
            let (Some(pid), Some(idx)) = (
                p.get("poll_id").and_then(Value::as_u64),
                p.get("option_index")
                    .and_then(Value::as_u64)
                    .map(|i| i as usize),
            ) else {
                return;
            };
            let event = {
                let mut inner = room.inner.lock().unwrap();
                if inner.effective_locked() && !is_admin {
                    return personal_err(ptx, "locked", "room is locked");
                }
                let Some((closed, n_opts)) = inner
                    .polls
                    .iter()
                    .find(|p| p.id == pid)
                    .map(|p| (p.closed, p.options.len()))
                else {
                    return;
                };
                if closed {
                    return personal_err(ptx, "poll_closed", "poll is closed");
                }
                if idx >= n_opts {
                    return personal_err(ptx, "bad_option", "invalid option");
                }
                let voters = inner.poll_voters.entry(pid).or_default();
                if !voters.insert(identity.to_string()) {
                    return personal_err(ptx, "already_voted", "you already voted");
                }
                let poll = inner.polls.iter_mut().find(|p| p.id == pid).unwrap();
                poll.options[idx].votes += 1;
                let options: Vec<Value> = poll
                    .options
                    .iter()
                    .map(|o| json!({ "text": o.text, "votes": o.votes }))
                    .collect();
                json!({ "type": "poll_update", "payload": { "poll_id": pid, "options": options } })
            };
            persist(
                app,
                &room.id,
                "poll_vote",
                json!({"poll_id": pid, "option_index": idx, "voter": identity}),
            );
            room.broadcast(event);
        }

        // ── admin actions ───────────────────────────────────────────────
        "admin_approve_message" => {
            let Some(mid) = p.get("message_id").and_then(Value::as_u64) else {
                return;
            };
            let event = {
                let mut inner = room.inner.lock().unwrap();
                let Some(pos) = inner.pending_messages.iter().position(|m| m.id == mid) else {
                    return;
                };
                let msg = inner.pending_messages.remove(pos);
                inner.messages.push_back(msg.clone());
                while inner.messages.len() > room.cfg.max_messages {
                    inner.messages.pop_front();
                }
                json!({ "type": "message", "payload": msg })
            };
            if let Some(p) = event.get("payload") {
                persist(
                    app,
                    &room.id,
                    "message",
                    json!({"id": p["id"], "role": p["role"], "text": p["text"], "ts": p["ts"]}),
                );
            }
            room.broadcast(event);
        }

        "admin_reject_message" => {
            let Some(mid) = p.get("message_id").and_then(Value::as_u64) else {
                return;
            };
            room.inner
                .lock()
                .unwrap()
                .pending_messages
                .retain(|m| m.id != mid);
            room.broadcast(json!({ "type": "pending_message_rejected", "payload": { "id": mid } }));
        }

        "admin_approve_question" => {
            let Some(qid) = p.get("question_id").and_then(Value::as_u64) else {
                return;
            };
            let event = {
                let mut inner = room.inner.lock().unwrap();
                let Some(pos) = inner.pending_questions.iter().position(|q| q.id == qid) else {
                    return;
                };
                let q = inner.pending_questions.remove(pos);
                inner.questions.push(q.clone());
                json!({ "type": "question", "payload": q })
            };
            if let Some(p) = event.get("payload") {
                persist(
                    app,
                    &room.id,
                    "question",
                    json!({"id": p["id"], "text": p["text"]}),
                );
            }
            room.broadcast(event);
        }

        "admin_reject_question" => {
            let Some(qid) = p.get("question_id").and_then(Value::as_u64) else {
                return;
            };
            room.inner
                .lock()
                .unwrap()
                .pending_questions
                .retain(|q| q.id != qid);
            room.broadcast(
                json!({ "type": "pending_question_rejected", "payload": { "id": qid } }),
            );
        }

        "admin_lock" => {
            let locked = p.get("locked").and_then(Value::as_bool).unwrap_or(false);
            // Clamp the duration so the deadline math can't overflow and a
            // timed lock can't be scheduled absurdly far out.
            let dur = p
                .get("duration_seconds")
                .and_then(Value::as_u64)
                .map(|d| d.min(86_400));
            let until = if locked {
                dur.map(|d| now_ms().saturating_add(d.saturating_mul(1000)))
            } else {
                None
            };

            let epoch = {
                let mut inner = room.inner.lock().unwrap();
                inner.locked = locked;
                inner.locked_until = until;
                inner.lock_epoch += 1;
                inner.lock_epoch
            };
            persist(
                app,
                &room.id,
                "lock",
                json!({"locked": locked, "until": until}),
            );
            room.broadcast(
                json!({ "type": "lock", "payload": { "locked": locked, "until": until } }),
            );

            // Schedule automatic unlock; it fires only if the lock epoch is
            // still the one we set (so a later manual lock/unlock wins). Holds
            // a Weak ref so the sleeping task never keeps a closed/expired room
            // alive.
            if let (true, Some(d)) = (locked, dur) {
                let weak = Arc::downgrade(room);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(d)).await;
                    let Some(room) = weak.upgrade() else { return };
                    room.update(|inner| {
                        if inner.lock_epoch == epoch && inner.locked {
                            inner.locked = false;
                            inner.locked_until = None;
                            inner.lock_epoch += 1;
                            Some(json!({ "type": "lock", "payload": { "locked": false } }))
                        } else {
                            None
                        }
                    });
                });
            }
        }

        "admin_delete_message" => {
            let Some(mid) = p.get("message_id").and_then(Value::as_u64) else {
                return;
            };
            {
                let mut inner = room.inner.lock().unwrap();
                inner.messages.retain(|m| m.id != mid);
                inner.reaction_users.retain(|(id, _), _| *id != mid);
            }
            persist(app, &room.id, "message_deleted", json!({"id": mid}));
            room.broadcast(json!({ "type": "message_deleted", "payload": { "id": mid } }));
        }

        "admin_pin_question" => {
            let Some(qid) = p.get("question_id").and_then(Value::as_u64) else {
                return;
            };
            let pinned = {
                let mut inner = room.inner.lock().unwrap();
                let already = inner.questions.iter().any(|q| q.id == qid && q.pinned);
                for q in inner.questions.iter_mut() {
                    q.pinned = if already { false } else { q.id == qid };
                }
                !already
            };
            persist(
                app,
                &room.id,
                "question_pinned",
                json!({"question_id": qid, "pinned": pinned}),
            );
            room.broadcast(json!({ "type": "question_pinned", "payload": { "question_id": qid, "pinned": pinned } }));
        }

        "admin_answer_question" => {
            let Some(qid) = p.get("question_id").and_then(Value::as_u64) else {
                return;
            };
            {
                let mut inner = room.inner.lock().unwrap();
                if let Some(q) = inner.questions.iter_mut().find(|q| q.id == qid) {
                    q.answered = true;
                    q.pinned = false;
                }
            }
            persist(
                app,
                &room.id,
                "question_answered",
                json!({"question_id": qid}),
            );
            room.broadcast(
                json!({ "type": "question_answered", "payload": { "question_id": qid } }),
            );
        }

        "admin_dismiss_question" => {
            let Some(qid) = p.get("question_id").and_then(Value::as_u64) else {
                return;
            };
            {
                let mut inner = room.inner.lock().unwrap();
                inner.questions.retain(|q| q.id != qid);
                inner.question_voters.remove(&qid);
            }
            persist(
                app,
                &room.id,
                "question_dismissed",
                json!({"question_id": qid}),
            );
            room.broadcast(
                json!({ "type": "question_dismissed", "payload": { "question_id": qid } }),
            );
        }

        "admin_create_poll" => {
            let question = p
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let options: Vec<String> = p
                .get("options")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o.as_str())
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            if question.is_empty() || !(2..=6).contains(&options.len()) {
                return personal_err(ptx, "bad_poll", "need a question and 2–6 options");
            }
            let poll = {
                let mut inner = room.inner.lock().unwrap();
                let id = inner.next_poll_id();
                let poll = Poll {
                    id,
                    question,
                    options: options
                        .into_iter()
                        .map(|text| PollOption { text, votes: 0 })
                        .collect(),
                    closed: false,
                };
                inner.polls.push(poll.clone());
                poll
            };
            persist(
                app,
                &room.id,
                "poll_created",
                json!({"id": poll.id, "question": poll.question,
                       "options": poll.options.iter().map(|o| json!({"text": o.text})).collect::<Vec<_>>()}),
            );
            room.broadcast(json!({ "type": "poll_created", "payload": poll }));
        }

        "admin_close_poll" => {
            let Some(pid) = p.get("poll_id").and_then(Value::as_u64) else {
                return;
            };
            {
                let mut inner = room.inner.lock().unwrap();
                if let Some(poll) = inner.polls.iter_mut().find(|p| p.id == pid) {
                    poll.closed = true;
                }
            }
            persist(app, &room.id, "poll_closed", json!({"poll_id": pid}));
            room.broadcast(json!({ "type": "poll_closed", "payload": { "poll_id": pid } }));
        }

        "admin_delete_poll" => {
            let Some(pid) = p.get("poll_id").and_then(Value::as_u64) else {
                return;
            };
            {
                let mut inner = room.inner.lock().unwrap();
                inner.polls.retain(|p| p.id != pid);
                inner.poll_voters.remove(&pid);
            }
            persist(app, &room.id, "poll_deleted", json!({"poll_id": pid}));
            room.broadcast(json!({ "type": "poll_deleted", "payload": { "poll_id": pid } }));
        }

        "admin_display_mode" => {
            let mode = p.get("mode").and_then(Value::as_str).unwrap_or("questions");
            if !["questions", "messages", "polls", "qr"].contains(&mode) {
                return;
            }
            room.broadcast(json!({ "type": "display_mode", "payload": { "mode": mode } }));
        }

        "admin_close_room" => {
            room.broadcast(json!({ "type": "room_closed", "payload": {} }));
            app.rooms.remove(&room.id);
            if let Some(pool) = app.db.as_ref() {
                let pool = pool.clone();
                let room_id = room.id.clone();
                tokio::spawn(async move { db::delete_room(&pool, &room_id).await });
            }
        }

        _ => personal_err(ptx, "unknown", "unknown message type"),
    }
}

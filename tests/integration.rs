//! Hermetic end-to-end tests. Each test mounts the real router on an ephemeral
//! 127.0.0.1 port, then drives it over HTTP + WebSocket exactly as a browser
//! would. No external services, no fixed ports, no shared state between tests.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as WsMessage,
};

use void::auth;
use void::config::Config;
use void::state::AppState;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Spin up a server on an ephemeral port and return its base URL.
async fn spawn() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Config {
        host: "127.0.0.1".into(),
        port: addr.port(),
        base_url: format!("http://{addr}"),
        bcrypt_cost: 4, // minimum cost — keep password tests fast
        default_ttl_seconds: 7200,
        max_ttl_seconds: 86400,
        default_max_messages: 200,
        max_messages_per_room: 1000,
        default_max_participants: 100,
        max_participants_per_room: 500,
        default_rate_limit_seconds: 0, // disable unless a test opts in
        max_message_length: 500,
        ttl_sweep_interval_seconds: 60,
    };
    let state = AppState::new(config);
    tokio::spawn(async move {
        axum::serve(listener, void::app(state)).await.unwrap();
    });
    format!("http://{addr}")
}

// ── HTTP helpers ────────────────────────────────────────────────────────────

async fn create_room(base: &str, body: Value) -> Value {
    reqwest::Client::new()
        .post(format!("{base}/rooms"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn room_id(created: &Value) -> String {
    created["room_id"].as_str().unwrap().to_string()
}
fn secret(created: &Value) -> String {
    created["admin_url"].as_str().unwrap().rsplit('/').next().unwrap().to_string()
}

/// Join as a user; returns (status, session_token option).
async fn join(base: &str, id: &str, password: Option<&str>) -> (u16, Option<String>) {
    let resp = reqwest::Client::new()
        .post(format!("{base}/r/{id}/join"))
        .json(&json!({ "password": password }))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let token = resp
        .json::<Value>()
        .await
        .ok()
        .and_then(|v| v["session_token"].as_str().map(str::to_string));
    (status, token)
}

// ── WebSocket helpers ─────────────────────────────────────────────────────────

async fn ws(base: &str, id: &str, token: &str) -> Ws {
    // Token rides in the Sec-WebSocket-Protocol header, not the URL.
    let url = format!("{}/r/{}/ws", base.replace("http", "ws"), id);
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_str(&format!("void.token.{token}")).unwrap(),
    );
    connect_async(req).await.unwrap().0
}

async fn send(ws: &mut Ws, kind: &str, payload: Value) {
    let msg = json!({ "type": kind, "payload": payload }).to_string();
    ws.send(WsMessage::Text(msg.into())).await.unwrap();
}

/// Next event, or None on a short timeout (no more events arriving).
async fn next_event(ws: &mut Ws) -> Option<Value> {
    loop {
        match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(WsMessage::Text(t)))) => return Some(serde_json::from_str(&t).unwrap()),
            Ok(Some(Ok(_))) => continue, // ping/pong/binary — skip
            _ => return None,
        }
    }
}

/// Wait for the next event of a given `type`, skipping others (e.g.
/// participant_count chatter). None if it doesn't arrive before timeout.
async fn wait_for(ws: &mut Ws, kind: &str) -> Option<Value> {
    while let Some(ev) = next_event(ws).await {
        if ev["type"] == kind {
            return Some(ev);
        }
    }
    None
}

/// GET the state snapshot with an admin bearer token.
async fn get_state(base: &str, id: &str, token: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{base}/r/{id}/state"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap()
}

async fn admin_ws(base: &str, created: &Value) -> Ws {
    let id = room_id(created);
    let token = auth::admin_token(&secret(created), &id);
    ws(base, &id, &token).await
}

async fn user_ws(base: &str, created: &Value) -> Ws {
    let id = room_id(created);
    let (status, token) = join(base, &id, None).await;
    assert_eq!(status, 200);
    ws(base, &id, &token.unwrap()).await
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn serves_embedded_frontend() {
    let base = spawn().await;
    let index = reqwest::get(format!("{base}/")).await.unwrap();
    assert_eq!(index.status(), 200);
    assert!(index.headers()["content-type"].to_str().unwrap().contains("html"));

    let js = reqwest::get(format!("{base}/assets/app.js")).await.unwrap();
    assert_eq!(js.status(), 200);
    assert!(reqwest::get(format!("{base}/assets/missing.js")).await.unwrap().status() == 404);
}

#[tokio::test]
async fn create_returns_links() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let id = room_id(&room);
    assert_eq!(id.len(), 6);
    assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(room["public_url"].as_str().unwrap().ends_with(&format!("/r/{id}")));
    assert!(room["admin_url"].as_str().unwrap().contains(&format!("/r/{id}/")));
}

#[tokio::test]
async fn password_gate() {
    let base = spawn().await;
    let room = create_room(&base, json!({ "password": "hunter2" })).await;
    let id = room_id(&room);

    assert_eq!(join(&base, &id, Some("wrong")).await.0, 403);
    assert_eq!(join(&base, &id, None).await.0, 403);
    let (status, token) = join(&base, &id, Some("hunter2")).await;
    assert_eq!(status, 200);
    assert!(token.is_some());
}

#[tokio::test]
async fn state_requires_auth() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let id = room_id(&room);

    let unauthed = reqwest::get(format!("{base}/r/{id}/state")).await.unwrap();
    assert_eq!(unauthed.status(), 403);

    let token = auth::admin_token(&secret(&room), &id);
    assert_eq!(get_state(&base, &id, &token).await.status(), 200);
    assert_eq!(get_state(&base, &id, "deadbeef").await.status(), 403);
}

#[tokio::test]
async fn message_broadcasts_to_all() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut admin = admin_ws(&base, &room).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut user, "message", json!({ "text": "hello void" })).await;

    let on_admin = wait_for(&mut admin, "message").await.unwrap();
    let on_user = wait_for(&mut user, "message").await.unwrap();
    assert_eq!(on_admin["payload"]["text"], "hello void");
    assert_eq!(on_admin["payload"]["role"], "user");
    assert_eq!(on_user["payload"]["text"], "hello void");
}

#[tokio::test]
async fn write_lock_blocks_users_not_admin() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut admin = admin_ws(&base, &room).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut admin, "admin_lock", json!({ "locked": true })).await;
    assert_eq!(wait_for(&mut user, "lock").await.unwrap()["payload"]["locked"], true);

    // User's message is rejected with a personal error, not broadcast.
    send(&mut user, "message", json!({ "text": "blocked?" })).await;
    assert_eq!(wait_for(&mut user, "error").await.unwrap()["payload"]["code"], "locked");

    // Admin can still post while locked.
    send(&mut admin, "message", json!({ "text": "presenter" })).await;
    assert_eq!(wait_for(&mut user, "message").await.unwrap()["payload"]["text"], "presenter");
}

#[tokio::test]
async fn rate_limit_enforced_per_session() {
    let base = spawn().await;
    let room = create_room(&base, json!({ "rate_limit_seconds": 10 })).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut user, "message", json!({ "text": "first" })).await;
    assert!(wait_for(&mut user, "message").await.is_some());

    send(&mut user, "message", json!({ "text": "too soon" })).await;
    assert_eq!(wait_for(&mut user, "error").await.unwrap()["payload"]["code"], "rate_limited");
}

#[tokio::test]
async fn message_length_capped() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut user = user_ws(&base, &room).await;

    let long = "x".repeat(501);
    send(&mut user, "message", json!({ "text": long })).await;
    assert_eq!(wait_for(&mut user, "error").await.unwrap()["payload"]["code"], "too_long");
}

#[tokio::test]
async fn question_one_vote_per_user() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut user, "question", json!({ "text": "why?" })).await;
    let q = wait_for(&mut user, "question").await.unwrap();
    let qid = q["payload"]["id"].as_u64().unwrap();

    send(&mut user, "vote", json!({ "question_id": qid })).await;
    assert_eq!(wait_for(&mut user, "vote").await.unwrap()["payload"]["votes"], 1);

    send(&mut user, "vote", json!({ "question_id": qid })).await;
    assert_eq!(wait_for(&mut user, "error").await.unwrap()["payload"]["code"], "already_voted");
}

#[tokio::test]
async fn poll_vote_once_then_close() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut admin = admin_ws(&base, &room).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut admin, "admin_create_poll", json!({ "question": "best?", "options": ["a", "b"] })).await;
    let poll = wait_for(&mut user, "poll_created").await.unwrap();
    let pid = poll["payload"]["id"].as_u64().unwrap();

    send(&mut user, "poll_vote", json!({ "poll_id": pid, "option_index": 1 })).await;
    let update = wait_for(&mut user, "poll_update").await.unwrap();
    assert_eq!(update["payload"]["options"][1]["votes"], 1);

    send(&mut user, "poll_vote", json!({ "poll_id": pid, "option_index": 0 })).await;
    assert_eq!(wait_for(&mut user, "error").await.unwrap()["payload"]["code"], "already_voted");

    send(&mut admin, "admin_close_poll", json!({ "poll_id": pid })).await;
    assert!(wait_for(&mut user, "poll_closed").await.is_some());
}

#[tokio::test]
async fn invalid_poll_rejected() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut admin = admin_ws(&base, &room).await;

    send(&mut admin, "admin_create_poll", json!({ "question": "q", "options": ["only one"] })).await;
    assert_eq!(wait_for(&mut admin, "error").await.unwrap()["payload"]["code"], "bad_poll");
}

#[tokio::test]
async fn reaction_toggles() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut user, "message", json!({ "text": "react to me" })).await;
    let mid = wait_for(&mut user, "message").await.unwrap()["payload"]["id"].as_u64().unwrap();

    send(&mut user, "reaction", json!({ "message_id": mid, "emoji": "🔥" })).await;
    assert_eq!(wait_for(&mut user, "reaction").await.unwrap()["payload"]["count"], 1);

    send(&mut user, "reaction", json!({ "message_id": mid, "emoji": "🔥" })).await;
    assert_eq!(wait_for(&mut user, "reaction").await.unwrap()["payload"]["count"], 0);
}

#[tokio::test]
async fn admin_actions_require_admin_token() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut user, "admin_lock", json!({ "locked": true })).await;
    assert_eq!(wait_for(&mut user, "error").await.unwrap()["payload"]["code"], "forbidden");
}

#[tokio::test]
async fn admin_delete_message() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut admin = admin_ws(&base, &room).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut user, "message", json!({ "text": "delete me" })).await;
    let mid = wait_for(&mut user, "message").await.unwrap()["payload"]["id"].as_u64().unwrap();

    send(&mut admin, "admin_delete_message", json!({ "message_id": mid })).await;
    assert_eq!(wait_for(&mut user, "message_deleted").await.unwrap()["payload"]["id"], mid);
}

#[tokio::test]
async fn close_room_evicts_and_blocks_rejoin() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let id = room_id(&room);
    let mut admin = admin_ws(&base, &room).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut admin, "admin_close_room", json!({})).await;
    assert!(wait_for(&mut user, "room_closed").await.is_some());

    // Room is gone — a fresh join fails.
    assert_eq!(join(&base, &id, None).await.0, 404);
}

#[tokio::test]
async fn message_history_capped() {
    let base = spawn().await;
    let room = create_room(&base, json!({ "max_messages": 3 })).await;
    let id = room_id(&room);
    let mut user = user_ws(&base, &room).await;

    for i in 0..5 {
        send(&mut user, "message", json!({ "text": format!("m{i}") })).await;
        wait_for(&mut user, "message").await.unwrap();
    }

    // Snapshot keeps only the last 3; oldest were dropped.
    let token = auth::admin_token(&secret(&room), &id);
    let state: Value = get_state(&base, &id, &token).await.json().await.unwrap();
    let msgs = state["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0]["text"], "m2");
    assert_eq!(msgs[2]["text"], "m4");
}

#[tokio::test]
async fn snapshot_is_first_ws_frame() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut user = user_ws(&base, &room).await;

    // The very first frame is the state snapshot (closes the cold-join race).
    let first = next_event(&mut user).await.unwrap();
    assert_eq!(first["type"], "snapshot");
    assert_eq!(first["payload"]["id"], room_id(&room));
}

#[tokio::test]
async fn two_admins_vote_independently() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    // Two distinct admin connections (spec: admin link in a second browser).
    let mut a1 = admin_ws(&base, &room).await;
    let mut a2 = admin_ws(&base, &room).await;

    send(&mut a1, "admin_create_poll", json!({ "question": "q", "options": ["x", "y"] })).await;
    let pid = wait_for(&mut a1, "poll_created").await.unwrap()["payload"]["id"].as_u64().unwrap();

    send(&mut a1, "poll_vote", json!({ "poll_id": pid, "option_index": 0 })).await;
    assert_eq!(wait_for(&mut a1, "poll_update").await.unwrap()["payload"]["options"][0]["votes"], 1);
    // Second admin is a separate identity, so its vote is NOT rejected — a1 sees
    // the count climb to 2 (a rejected duplicate would yield no further update).
    send(&mut a2, "poll_vote", json!({ "poll_id": pid, "option_index": 0 })).await;
    assert_eq!(wait_for(&mut a1, "poll_update").await.unwrap()["payload"]["options"][0]["votes"], 2);
}

#[tokio::test]
async fn timed_lock_auto_unlocks() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let mut admin = admin_ws(&base, &room).await;
    let mut user = user_ws(&base, &room).await;

    send(&mut admin, "admin_lock", json!({ "locked": true, "duration_seconds": 1 })).await;
    assert_eq!(wait_for(&mut user, "lock").await.unwrap()["payload"]["locked"], true);

    // The auto-unlock event arrives once the timer fires (~1s).
    let unlock = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(ev) = next_event(&mut user).await {
                if ev["type"] == "lock" && ev["payload"]["locked"] == false {
                    return true;
                }
            }
        }
    })
    .await;
    assert_eq!(unlock, Ok(true));
}

#[tokio::test]
async fn state_bearer_scheme_case_insensitive() {
    let base = spawn().await;
    let room = create_room(&base, json!({})).await;
    let id = room_id(&room);
    let token = auth::admin_token(&secret(&room), &id);

    // Lowercase scheme (RFC 7235 says case-insensitive) is accepted.
    let resp = reqwest::Client::new()
        .get(format!("{base}/r/{id}/state"))
        .header("authorization", format!("bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A query-param token is NOT accepted — it must never be a usable channel.
    let via_query = reqwest::get(format!("{base}/r/{id}/state?token={token}")).await.unwrap();
    assert_eq!(via_query.status(), 403);
}

#[tokio::test]
async fn session_reconnect_within_ttl() {
    let base = spawn().await;
    let room = create_room(&base, json!({ "password": "p" })).await;
    let id = room_id(&room);

    // First join with password yields a session token.
    let (status, token) = join(&base, &id, Some("p")).await;
    assert_eq!(status, 200);
    let token = token.unwrap();

    // Reconnect with that session, no password — accepted.
    let resp = reqwest::Client::new()
        .post(format!("{base}/r/{id}/join"))
        .json(&json!({ "session": token }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["session_token"], token);
}

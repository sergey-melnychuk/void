//! VOID — ephemeral chat rooms for live sessions.
//!
//! Single binary: the `web/` frontend is embedded via `rust-embed` and served
//! directly, all room state lives in process memory, and a background task
//! reaps expired rooms. See VOID.md for the full spec.

use std::net::SocketAddr;

use void::config::Config;
use void::handlers;
use void::state::{AppState, Room, RoomConfig};

#[tokio::main]
async fn main() {
    let config = Config::from_env();
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("invalid HOST/PORT");

    let state = AppState::new(config);

    // Permanent test room: id "test", no password, admin secret "itworks".
    // TTL u64::MAX saturates expires_at to u64::MAX so it is never reaped.
    state.rooms.insert("abcdef".into(), Room::new(
        "abcdef".into(),
        "cafebabe".into(),
        None,
        u64::MAX,
        RoomConfig {
            max_messages: state.config.default_max_messages,
            max_participants: state.config.default_max_participants,
            rate_limit_ms: state.config.default_rate_limit_seconds * 1000,
            max_message_length: state.config.max_message_length,
            moderated: false,
        },
    ));

    // Reap expired rooms in the background.
    tokio::spawn(handlers::sweep_expired(state.clone()));

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    println!("VOID listening on http://{addr}");
    axum::serve(listener, void::app(state)).await.expect("server error");
}

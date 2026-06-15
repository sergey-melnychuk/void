//! VOID — ephemeral chat rooms for live sessions.
//!
//! Single binary: the `web/` frontend is embedded via `rust-embed` and served
//! directly, room state lives in process memory (with optional Postgres
//! persistence), and a background task reaps expired rooms. See VOID.md.

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

    // Connect to Postgres and run pending migrations, if DATABASE_URL is set.
    let db = if let Some(url) = &config.database_url {
        match void::db::connect(url).await {
            Ok(pool) => {
                void::db::run_migrations(&pool)
                    .await
                    .expect("migration failed");
                println!("VOID db connected and migrations applied");
                Some(pool)
            }
            Err(e) => {
                eprintln!("VOID db connection failed: {e}");
                std::process::exit(1);
            }
        }
    } else {
        println!("VOID running without persistence (no DATABASE_URL)");
        None
    };

    let state = AppState::new(config, db.clone());

    // Replay persisted rooms into memory before accepting traffic.
    if let Some(pool) = &db {
        for room in void::db::replay_all(pool, &state.config).await {
            state.rooms.insert(room.id.clone(), room);
        }
        println!("VOID replayed {} rooms from db", state.rooms.len());
    }

    // Permanent test room: always fresh (never persisted), TTL = never.
    state.rooms.insert(
        "test".into(),
        Room::new(
            "test".into(),
            None,
            "test".into(),
            None,
            u64::MAX, // expires_at sentinel = never
            RoomConfig {
                max_messages: state.config.default_max_messages,
                max_participants: state.config.default_max_participants,
                rate_limit_ms: state.config.default_rate_limit_seconds * 1000,
                max_message_length: state.config.max_message_length,
                moderated: false,
            },
        ),
    );

    // Reap expired rooms in the background.
    tokio::spawn(handlers::sweep_expired(state.clone()));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");
    println!("VOID listening on http://{addr}");
    axum::serve(listener, void::app(state))
        .await
        .expect("server error");
}

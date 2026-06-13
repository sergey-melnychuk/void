//! VOID library surface. The binary (`main.rs`) is a thin wrapper; integration
//! tests mount [`app`] directly on an ephemeral port.

pub mod auth;
pub mod config;
pub mod handlers;
pub mod state;
pub mod ws;

use std::sync::Arc;

use axum::{
    Router,
    extract::Path,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rust_embed::RustEmbed;

use crate::state::AppState;

/// The entire `web/` directory is compiled into the binary at build time, so
/// the shippable artifact is a single executable — no static files to deploy.
#[derive(RustEmbed)]
#[folder = "web/"]
struct Web;

/// Build the full application router given shared state.
pub fn app(state: Arc<AppState>) -> Router {
    Router::new()
        // SPA shell — served for the landing page and any room URL (public or
        // admin); the frontend reads the path and routes client-side.
        .route("/", get(index))
        .route("/r/{room_id}", get(index))
        .route("/r/{room_id}/{secret}", get(index))
        // REST.
        .route("/rooms", post(handlers::create_room))
        .route("/r/{room_id}/join", post(handlers::join_room))
        .route("/r/{room_id}/state", get(handlers::room_state))
        // WebSocket.
        .route("/r/{room_id}/ws", get(ws::ws_handler))
        // Embedded static assets.
        .route("/assets/{*path}", get(assets))
        .with_state(state)
}

async fn index() -> Response {
    serve_embedded("index.html")
}

async fn assets(Path(path): Path<String>) -> Response {
    serve_embedded(&format!("assets/{path}"))
}

/// Look up `path` in the embedded `web/` tree, returning it with a guessed
/// content type, or 404.
fn serve_embedded(path: &str) -> Response {
    match Web::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], content.data.into_owned()).into_response()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

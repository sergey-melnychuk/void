# VOID

Ephemeral chat rooms for live sessions — conference talks, workshops, Q&A.

A presenter spins up a room, drops a short link on a slide, and the whole thing
evaporates when the session ends. No accounts, no database, no persistence, no
noise.

## Why

Live audiences need a back-channel: chat, questions, quick polls. Existing tools
demand sign-ups, retain data forever, or are heavyweight to run. VOID is a single
binary you can deploy in minutes. State lives in memory and is wiped on a TTL —
nothing to clean up, no PII to leak.

## Features

- **Rooms in one request** — `POST /rooms`, no account. Optional password, a TTL
  (default 2h, max 24h), and a hard cap on participants and message history.
- **Two roles, no identity** — `admin` (holds the secret link) and `user`
  (everyone else). Messages are anonymous; only a role badge is shown.
- **Real-time chat** — WebSocket broadcast, per-user rate limiting, 500-char cap,
  emoji reactions, admin message deletion, and a global write lock (timed or
  indefinite) for talk-then-Q&A flow.
- **Q&A board** — attendees submit questions, upvote (one vote each); admin pins
  the one being answered and dismisses the rest.
- **Polls** — admin-created multiple choice (2–6 options) with live results;
  one vote per user, enforced server-side.

## How it works

- **Public link** — `void.sh/r/<room_id>` (6 hex chars). Safe to share on a slide.
- **Private link** — `void.sh/r/<room_id>/<secret>` (16-byte secret). Shown once at
  creation; whoever holds it is admin. The admin token is derived
  `HMAC-SHA256(secret, room_id)` and recomputed server-side on every privileged
  action — never stored.

All room state (messages, questions, polls, participants) lives entirely in
process memory. A background task sweeps expired rooms. Restart the server and
ephemeral rooms are gone by design.

## Tech stack

| Layer | Choice |
|---|---|
| Backend | Rust / [Axum](https://github.com/tokio-rs/axum) — JSON API + WebSocket |
| State | In-process `DashMap`, `VecDeque` per room for capped history |
| Broadcast | `tokio::sync::broadcast`, one channel per room |
| Frontend | Vue 3 via CDN (`esm.sh`), Composition API — no build step |
| Assets | `rust-embed` — static HTML/JS/CSS bundled into the binary, single-file deploy |
| Config | ENV vars (12-factor) |

No database, no Redis, no external services in v1. No frontend toolchain either —
the UI is plain files served straight from the binary. `cargo build` is the only
build step. Single binary, target < 20MB.

## API surface

REST is minimal; everything after join goes over WebSocket.

```
POST  /rooms              Create room → { room_id, public_url, admin_url }
GET   /r/:room_id         Serve the SPA
GET   /r/:room_id/state   Initial room state snapshot
WS    /r/:room_id/ws      WebSocket; ?token=<session|admin>
GET   /assets/*           Static JS/CSS
```

See [VOID.md](./VOID.md) for the full WebSocket protocol, security model, and
test scenarios.

## Development

```sh
cargo run
```

That's it — no Node, no npm, no bundler. The frontend is static files
(`index.html` + Vue loaded from a CDN) embedded via `rust-embed` and served by
the binary. Edit the files and rebuild.

## Configuration

Configured entirely through environment variables:

```
HOST=0.0.0.0
PORT=8080
BASE_URL=https://void.sh
DEFAULT_TTL_SECONDS=7200        # max 86400 (24h)
DEFAULT_MAX_MESSAGES=200        # max 1000
DEFAULT_MAX_PARTICIPANTS=100    # max 500
DEFAULT_RATE_LIMIT_SECONDS=3
MAX_MESSAGE_LENGTH=500
TTL_SWEEP_INTERVAL_SECONDS=60
```

See [VOID.md](./VOID.md#deployment) for the full list and deployment notes
(Fly.io, Heroku, Render, Docker, self-hosted).

## Deployment

Single binary or container. Fly.io is recommended (WebSockets out of the box, no
add-ons, no dyno sleep):

```sh
fly deploy
```

In-memory state is lost on restart — acceptable and intended for ephemeral rooms.
Single instance only in v1; multi-instance scaling is future work.

## Status

v1 — specification in [VOID.md](./VOID.md). Implementation in progress.

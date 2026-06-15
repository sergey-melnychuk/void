# VOID

Ephemeral chat rooms for live sessions — conference talks, workshops, Q&A.

A presenter spins up a room, drops a short link on a slide, and the whole thing
evaporates when the session ends. No accounts, no noise.

Live at **[0xff.wtf](https://0xff.wtf)**.

## Why

Live audiences need a back-channel: chat, questions, quick polls. Existing tools
demand sign-ups, retain data forever, or cost a fortune. VOID is a single binary
you can deploy in minutes. Rooms are ephemeral by design — nothing to clean up,
no PII to leak.

## Features

### Rooms
- **One request to create** — no account. Optional password, TTL, participant cap, rate limit, optional title.
- **Two tiers** — free (3h, 20 participants, 100 messages, 10 questions, 1 poll) and paid ($20/day, 30-day TTL, custom URL slug, unlimited everything, recap export on expiry).
- **Two roles** — `admin` (holds the secret link) and `user` (everyone else). Messages are anonymous; only a role badge is shown.
- **Moderated mode** — admin approves every message and question before it's visible to others.
- **Write lock** — admin can freeze chat (read-only); attendees see a banner.
- **Manual close** — admin can close the room early; all clients notified.

### Chat
- Real-time WebSocket broadcast
- Per-user rate limiting (server-enforced)
- 500-char message cap
- Emoji reactions (👍 ❤️ 😂 🔥 🤔) with toggle — one reaction entry per user per emoji
- Admin message deletion
- Pending state for moderated rooms (submitter sees dim "awaiting approval", red if dropped)

### Q&A
- Attendees submit questions; sorted by votes descending
- **Upvote / un-vote** — toggle; one vote per user, enforced server-side
- **Admin pin** — pinned question floats to top regardless of votes
- **Admin answer** — marks question as answered; sinks to bottom, dimmed, no more votes
- **Admin dismiss** — removes question entirely
- Moderation support (pending queue for admin)

### Polls
- Admin-created, 2–6 options, live results
- One vote per user, enforced server-side
- Admin close (freezes results) and delete
- Multiple polls per room

### Display window
- Admin opens a detached presentation window (`/w/<room_id>`) — same 3-column view without any controls
- QR code widget fixed to the bottom-right corner (public room URL, full column width, capped at 50% height)
- Press **F** to toggle fullscreen
- Display connections are invisible: not counted in presence, all input server-rejected

### Admin controls
- Lock / unlock room
- Close room
- Approve / reject pending messages and questions (moderated mode)
- Pin / answer / dismiss questions
- Create / close / delete polls
- Open display window

## How it works

- **Public link** — `0xff.wtf/r/<room_id>` — share on a slide
- **Admin link** — `0xff.wtf/r/<room_id>/<secret>` — shown once at creation; whoever holds it is admin
- **Admin token** — derived as `HMAC-SHA256(secret, room_id)`; recomputed server-side on every privileged action; never stored
- **Display link** — `0xff.wtf/w/<room_id>` — read-only presentation view, no controls

Session tokens are minted on join, stored in `localStorage`, and reused for reconnect within TTL. No cookies.

## Tech stack

| Layer | Choice |
|---|---|
| Backend | Rust / Axum — JSON REST + WebSocket |
| State | In-process `DashMap`; `tokio::sync::broadcast` per room |
| Persistence | Postgres (Neon) + SQLx — event log, survives restarts |
| Frontend | Vue 3 via CDN (`esm.sh`), Composition API — **no build step** |
| Assets | `rust-embed` — HTML/JS/CSS bundled into the binary |
| TLS | Caddy — automatic Let's Encrypt |
| Config | ENV vars (12-factor) |

Single binary. `cargo build` is the only build step. No Node, no npm, no bundler.

## API surface

```
POST  /rooms                Create room → { room_id, public_url, admin_url }
POST  /r/:id/join           Join room → { session_token, state }
GET   /r/:id/state          Admin state snapshot (requires admin token)
WS    /r/:id/ws             WebSocket — token in Sec-WebSocket-Protocol header
GET   /r/:id                Serve SPA
GET   /r/:id/:secret        Serve SPA (admin entry)
GET   /w/:id                Serve SPA (display window entry)
GET   /assets/*             Embedded static assets
```

See [VOID.md](./VOID.md) for the full WebSocket protocol and security model.

## Development

```sh
cargo run
```

Test room always available at `localhost:8080/r/test/test` (admin).

## Configuration

```
HOST=0.0.0.0
PORT=8080
BASE_URL=https://0xff.wtf
DATABASE_URL=postgres://...         # Neon connection string
BCRYPT_COST=10
DEFAULT_TTL_SECONDS=7200
DEFAULT_MAX_MESSAGES=200
DEFAULT_MAX_PARTICIPANTS=100
DEFAULT_RATE_LIMIT_SECONDS=3
MAX_MESSAGE_LENGTH=500
TTL_SWEEP_INTERVAL_SECONDS=60
```

## Deployment

Single binary behind Caddy on a VPS (currently DigitalOcean). Caddy handles TLS automatically.

```
/etc/caddy/Caddyfile:

0xff.wtf {
    reverse_proxy localhost:8080
}
```

Systemd unit keeps the binary running and restarts on crash. Room state survives
restarts via Postgres event log; in-memory structures are replayed on startup.

## Status

Live at [0xff.wtf](https://0xff.wtf). Persistence layer (Neon/SQLx) in progress.
See [paid-vs-free.md](./paid-vs-free.md) for tier details.

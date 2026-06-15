# VOID — Ephemeral Chat Rooms for Live Sessions

## Overview

VOID is a lightweight, ephemeral chat room service for conference talks, workshops, and live
sessions. A presenter creates a room, shares a short link, and the room disappears when the
session ends. No accounts, no noise.

Single Rust binary: `web/` assets embedded via `rust-embed`, room state in process memory
with optional Postgres persistence, background TTL sweep. Zero runtime dependencies to deploy.

---

## Current Implementation Status

### Implemented and working in production

- Room creation with configurable TTL, password, participant cap, message cap, rate limit, moderated mode
- WebSocket fan-out via `tokio::sync::broadcast` (per-room channel)
- Admin token derived client-side (HMAC-SHA256), validated server-side on every privileged action
- Chat: messages, reactions (👍 ❤️ 😂 🔥 🤔), per-user rate limiting, write lock (timed or permanent)
- Q&A board: questions, upvoting (toggle), pin, answer, dismiss
- Polls: create, live vote counts, close, delete
- Moderated mode: pending queue for messages and questions; admin approve/reject; queue persisted across restarts
- Display window (`/w/<id>`): read-only projection view with live QR code widget
- Room state survives server restarts (Postgres event sourcing, startup replay)
- IP-based room creation rate limiting
- Room sweep: in-memory reap + DB delete on TTL expiry

### Planned / not yet built

- **Topics** — `topics: Vec<String>` on Room; admin sets topics at creation or live; each
  message/question tagged with a topic; free users filter to one topic, paid users subscribe
  to multiple
- **Paid tier** — custom URL slugs (verified by email domain + manual approval, $50/day),
  30-day TTL, unlimited caps, recap export (JSON/PDF) on expiry ($20/day/room)
- **Email ownership** — paid admin can reset admin secret via email; prevents permanent lockout
- **Payment integration** — Stripe + crypto; day = 3h free trial + 24h paid, starts at payment second
- **Monitoring/alerting** — health endpoint, structured logs, error aggregation (future)
- **Multi-instance** — sticky sessions by room_id, Postgres NOTIFY or Redis for cross-instance broadcast

---

## Tech Stack

| Layer | Choice | Notes |
|---|---|---|
| Backend | Rust / Axum | Async, single binary |
| State | In-process `DashMap<String, Arc<Room>>` | `VecDeque<Message>` per room for capped history |
| Broadcast | `tokio::sync::broadcast` per room | + `admin_tx` for pending-item notifications |
| Frontend | Vue 3 via CDN, no build step | Single `web/assets/app.js`; resolved via import map in `index.html` |
| Styling | Vanilla CSS, custom properties | Dark theme, responsive; `web/assets/style.css` |
| Static assets | `rust-embed` (`web/` folder) | Binary embeds all HTML/JS/CSS; no files to deploy |
| Persistence | Postgres via SQLx 0.9 | Optional; fire-and-forget writes; startup event replay |
| Config | ENV vars | All have defaults; zero-config local run |
| License | BSL 1.1 | Free for non-commercial self-hosting <20 participants; converts to MIT 2030-01-01 |

### File layout

```
src/
  main.rs        — startup: config, DB connect+migrate, replay, test room, sweep, serve
  lib.rs         — router definition, rust-embed mount
  config.rs      — Config struct, ENV var parsing, clamp helpers
  state.rs       — AppState, Room, RoomInner, RoomConfig, apply_event (replay logic)
  handlers.rs    — POST /rooms, POST /r/:id/join, GET /r/:id/state, sweep_expired task
  ws.rs          — WebSocket handler, handle_client (full protocol), persist() helper
  auth.rs        — bcrypt hash/verify, admin token (HMAC-SHA256), room ID / secret / session token generation
  db.rs          — connect, run_migrations, persist_room, append_event, delete_room, delete_expired, replay_all
web/
  index.html     — SPA shell with Vue 3 import map
  assets/
    app.js       — all frontend logic and template (Vue 3 Composition API, single file)
    style.css    — dark theme, responsive layout
db/
  0001_initial.sql  — migration: rooms + events tables, indexes
```

---

## Configuration (ENV vars)

All have built-in defaults; binary starts with zero config.

| Variable | Default | Notes |
|---|---|---|
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8080` | Bind port |
| `BASE_URL` | `http://localhost:8080` | Prefix for generated room URLs |
| `DATABASE_URL` | _(unset)_ | Postgres URL; omit for pure in-memory mode |
| `BCRYPT_COST` | `10` | bcrypt work factor for room passwords |
| `DEFAULT_TTL_SECONDS` | `7200` | Room TTL when not specified (2 h) |
| `MAX_TTL_SECONDS` | `86400` | Hard cap (24 h) |
| `DEFAULT_MAX_MESSAGES` | `200` | Messages kept per room |
| `MAX_MESSAGES_PER_ROOM` | `1000` | Hard cap |
| `DEFAULT_MAX_PARTICIPANTS` | `100` | Concurrent WS connections per room |
| `MAX_PARTICIPANTS_PER_ROOM` | `500` | Hard cap |
| `DEFAULT_RATE_LIMIT_SECONDS` | `3` | Min gap between messages per session |
| `MAX_MESSAGE_LENGTH` | `500` | Characters per message (also enforced on questions) |
| `TTL_SWEEP_INTERVAL_SECONDS` | `60` | Background reap frequency |
| `ROOM_CREATION_LIMIT` | `10` | Max rooms per IP per window |
| `ROOM_CREATION_WINDOW_SECONDS` | `3600` | Sliding window for creation rate limit |

---

## API Surface

### REST

```
POST  /rooms              Create room → { room_id, public_url, admin_url }
POST  /r/:id/join         Join room → { session_token, state }
GET   /r/:id/state        Admin-only state snapshot (Authorization: Bearer <admin_token>)
GET   /r/:id              Serve SPA shell (index.html)
GET   /r/:id/:secret      Serve SPA shell (frontend reads secret from URL, derives admin token)
GET   /w/:id              Serve SPA shell (display window path)
WS    /r/:id/ws           WebSocket connection
GET   /assets/*           Embedded static assets
```

**Room creation** (`POST /rooms`) body (all optional):

```json
{
  "ttl_seconds": 7200,
  "title": "RustConf 2026",
  "password": "secret",
  "max_participants": 100,
  "max_messages": 200,
  "rate_limit_seconds": 3,
  "moderated": false
}
```

Response:

```json
{
  "room_id": "a3f9c2",
  "public_url": "https://0xff.wtf/r/a3f9c2",
  "admin_url": "https://0xff.wtf/r/a3f9c2/8f3kQpXmN2vLzR9w"
}
```

The admin URL is returned once and never retrievable again.

**Join** (`POST /r/:id/join`) body:

```json
{ "session": "<existing_token_or_null>", "password": "optional", "display": false }
```

Response includes `session_token` (stored in `localStorage`) and `state` (same shape as snapshot payload).

### Auth model

- **Admin token**: derived client-side — `HMAC-SHA256(key=secret, msg=room_id)` as lowercase hex.
  Secret comes from the URL segment (`/r/:id/:secret`). Server recomputes and compares with `ct_eq`
  on every privileged WS action. Token is never stored anywhere.
- **Session token**: server-minted 16-byte random hex on join. Stored in `localStorage`, sent via
  `Sec-WebSocket-Protocol: void.token.<token>` on WS connect. Admitted to room's `sessions` set;
  persists in-memory (not in DB) — resets on server restart, requiring re-join.
- **Display token**: prefixed `disp.<random>`, admitted but invisible (no participant count,
  read-only, no cap enforcement).

---

## WebSocket Protocol

Token travels in the `Sec-WebSocket-Protocol` header (never in the URL — stays out of access logs).

### Server → Client

| `type` | Key payload fields | Notes |
|---|---|---|
| `snapshot` | full room state object | First frame on connect; also sent on lag-resync |
| `message` | `{ id, role, text, ts, reactions }` | New or approved chat message |
| `message_deleted` | `{ id }` | |
| `reaction` | `{ message_id, emoji, count }` | count=0 means removed |
| `question` | `{ id, text, votes, pinned, answered }` | New or approved question |
| `vote` | `{ question_id, votes }` | |
| `question_pinned` | `{ question_id, pinned }` | pinned=false unpins |
| `question_answered` | `{ question_id }` | Marks answered, unpins |
| `question_dismissed` | `{ question_id }` | |
| `poll_created` | `{ id, question, options: [{text,votes}], closed }` | |
| `poll_update` | `{ poll_id, options: [{text,votes}] }` | After each vote |
| `poll_closed` | `{ poll_id }` | |
| `poll_deleted` | `{ poll_id }` | |
| `pending_message` | `{ id, role, text, ts, reactions }` | Sent to submitter (personal) + admin channel |
| `pending_message_rejected` | `{ id }` | Broadcast to all (submitter shows "dropped") |
| `pending_question` | `{ id, text, votes, pinned, answered }` | Same routing as pending_message |
| `pending_question_rejected` | `{ id }` | |
| `lock` | `{ locked, until? }` | `until` = epoch ms for timed lock |
| `room_closed` | `{}` | Room closed by admin or TTL sweep |
| `participant_count` | `{ count }` | On every connect/disconnect |
| `display_mode` | `{ mode }` | Admin-driven display panel switch |
| `error` | `{ code, message }` | Personal to sender only |

### Client → Server

| `type` | Key payload fields | Auth |
|---|---|---|
| `message` | `{ text }` | session |
| `reaction` | `{ message_id, emoji }` | session |
| `question` | `{ text }` | session |
| `vote` | `{ question_id }` | session (toggle) |
| `poll_vote` | `{ poll_id, option_index }` | session (once per poll) |
| `admin_lock` | `{ locked, duration_seconds? }` | admin |
| `admin_delete_message` | `{ message_id }` | admin |
| `admin_approve_message` | `{ message_id }` | admin |
| `admin_reject_message` | `{ message_id }` | admin |
| `admin_pin_question` | `{ question_id }` | admin (toggle) |
| `admin_answer_question` | `{ question_id }` | admin |
| `admin_dismiss_question` | `{ question_id }` | admin |
| `admin_approve_question` | `{ question_id }` | admin |
| `admin_reject_question` | `{ question_id }` | admin |
| `admin_create_poll` | `{ question, options: [string] }` | admin (2–6 options) |
| `admin_close_poll` | `{ poll_id }` | admin |
| `admin_delete_poll` | `{ poll_id }` | admin |
| `admin_display_mode` | `{ mode }` | admin (`questions`\|`messages`\|`polls`\|`qr`) |
| `admin_close_room` | `{}` | admin |

---

## Persistence

### Architecture

Postgres event sourcing. Two tables only. All writes are fire-and-forget (spawned tasks) so the DB
is never in the broadcast hot path.

**Schema** (`db/0001_initial.sql`):

```sql
CREATE TABLE rooms (
  id                  TEXT    PRIMARY KEY,
  title               TEXT,
  secret              TEXT    NOT NULL,
  password_hash       TEXT,
  expires_at          BIGINT,          -- epoch ms; NULL = never (test room)
  max_messages        INT     NOT NULL,
  max_participants    INT     NOT NULL,
  rate_limit_ms       BIGINT  NOT NULL,
  max_message_length  INT     NOT NULL,
  moderated           BOOLEAN NOT NULL
);

CREATE TABLE events (
  id       BIGSERIAL PRIMARY KEY,      -- ordering key for replay
  room_id  TEXT      NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  ts       BIGINT    NOT NULL,         -- epoch ms
  kind     TEXT      NOT NULL,
  payload  TEXT      NOT NULL          -- JSON string
);

CREATE INDEX ON events (room_id, id);  -- fast ordered replay per room
```

### Event kinds stored in DB

| kind | payload fields |
|---|---|
| `message` | `{ id, role, text, ts }` |
| `message_deleted` | `{ id }` |
| `reaction_add` | `{ message_id, emoji, voter }` |
| `reaction_remove` | `{ message_id, emoji, voter }` |
| `pending_message` | `{ id, role, text, ts }` |
| `pending_message_rejected` | `{ id }` |
| `question` | `{ id, text }` |
| `vote_add` | `{ question_id, voter }` |
| `vote_remove` | `{ question_id, voter }` |
| `question_pinned` | `{ question_id, pinned }` |
| `question_answered` | `{ question_id }` |
| `question_dismissed` | `{ question_id }` |
| `pending_question` | `{ id, text }` |
| `pending_question_rejected` | `{ id }` |
| `poll_created` | `{ id, question, options: [{text}] }` |
| `poll_vote` | `{ poll_id, option_index, voter }` |
| `poll_closed` | `{ poll_id }` |
| `poll_deleted` | `{ poll_id }` |
| `lock` | `{ locked, until }` |

`pending_message` + `pending_message_rejected` pair: on replay, a `message` event with the same id
removes the pending entry (approve path). A `pending_message_rejected` event drops it directly.

Timed lock auto-unlock is NOT persisted — `effective_locked()` checks `locked_until` vs wall clock
on every access, so a lock whose deadline passed during downtime auto-expires on first read.

Session tokens are NOT persisted — users must rejoin after a server restart.

### Startup replay

1. Load all non-expired room rows from `rooms`.
2. For each room, `SELECT kind, payload FROM events WHERE room_id = $1 ORDER BY id`.
3. Apply each event via `RoomInner::apply_event` to reconstruct full in-memory state.

### Write strategy

1. Mutate in-memory state
2. Broadcast to WebSocket subscribers
3. `tokio::spawn` async DB write (never awaited before broadcast)

A crash between steps 2 and 3 loses at most one in-flight event — acceptable.

---

## Deployment

### Production (Ubuntu VPS)

**Build and install:**

```bash
cargo build --release
sudo cp target/release/void /usr/local/bin/void
```

Static assets are baked into the binary. CSS/JS changes require a rebuild.

**Local Postgres:**

```bash
sudo apt install postgresql
sudo -u postgres createdb void
sudo -u postgres createuser --superuser ext   # match the OS user void runs as
```

**systemd unit** (`/etc/systemd/system/void.service`):

```ini
[Unit]
Description=0xff.wtf
After=network.target

[Service]
ExecStart=/usr/local/bin/void
Restart=always
RestartSec=5
User=ext
Environment=HOST=127.0.0.1
Environment=PORT=8080
Environment=BASE_URL=https://0xff.wtf
Environment=DATABASE_URL=postgresql:///void?host=/var/run/postgresql&user=ext

[Install]
WantedBy=multi-user.target
```

For secrets, use an `EnvironmentFile=/etc/void/env` (mode `600`) instead of inline `Environment=`.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now void
```

**Nginx** (TLS termination + WebSocket upgrade):

```nginx
server {
    listen 443 ssl;
    server_name 0xff.wtf;

    ssl_certificate     /etc/letsencrypt/live/0xff.wtf/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/0xff.wtf/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $remote_addr;   # required for IP rate limiting
    }
}
```

**TLS:**

```bash
apt install certbot python3-certbot-nginx
certbot --nginx -d 0xff.wtf
```

### Docker

```dockerfile
FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/void /usr/local/bin/void
EXPOSE 8080
CMD ["void"]
```

---

## Troubleshooting

**Service fails to start:**

```bash
sudo journalctl -u void -n 50
```

**`Peer authentication failed for user "anonymous"`** — sqlx defaults username to `"anonymous"`.
Fix: add `&user=<os-user>` to `DATABASE_URL`.

**`Peer authentication failed for user "ext"` (works in shell but not service)** — service runs as
wrong OS user. Add `User=ext` to `[Service]` and `daemon-reload`.

**`migration failed` on startup** — `db/` is embedded at compile time via `sqlx::migrate!("./db")`.
Rebuild from repo root.

**Room state not surviving restart** — verify `DATABASE_URL` is in service env:
`sudo systemctl show void --property=Environment`. Look for `VOID db connected and migrations
applied` in logs.

**WebSocket connections failing (nginx)** — ensure both `Upgrade` and `Connection: upgrade` headers
are forwarded (see nginx config above).

---

## Known Limitations / Design Decisions

- **Session tokens reset on restart** — users must rejoin; no persistent session store.
- **`myVotes` is client-side only** — vote highlight resets on page refresh (server count is always
  correct; it's just the UI indicator that forgets).
- **Single instance** — no cross-instance broadcast; scale vertically or add sticky routing + Postgres
  NOTIFY when needed.
- **No recap** — room state is wiped on expiry; paid tier recap export not yet built.
- **Test room** (`/r/test`) — always exists, never persisted, used for development.
- **Sessions not persisted** — intentional; prevents indefinite session accumulation without accounts.

---

## Free vs Paid (planned)

See `paid-vs-free.md` for the full comparison. Key differences:

| | Free | Paid |
|---|---|---|
| TTL | up to 3 h | up to 30 days |
| URL | random hex | custom slug (email-verified, manual approval) |
| Participants | max 20 | unlimited |
| Topics | one at a time | multiple simultaneous |
| Ownership | admin URL only | email on file, secret reset available |
| Price | free | $20/day/room; $50/day for custom slug |

---

## Future Work (priority order)

1. **Topics** — `topics: Vec<String>` on `Room` (set at creation or via `admin_set_topics` action);
   `topic: Option<String>` on `Message` and `Question`; client filters/subscribes per topic;
   free users select one topic, paid users multi-select. DB: add `topics TEXT[]` to rooms,
   `topic TEXT` to event payloads.

2. **Paid tier + payments** — Stripe integration; custom slug creation flow (email OTP for domain
   verification, e.g. `*@eurorust.eu` for slug `eurorust2026`); 30-day room TTL; unlimited caps.

3. **Recap export** — on room expiry (paid only), dump all events to JSON/PDF and email to owner.

4. **Email ownership** — store admin email at room creation (paid); allow admin secret reset via
   email OTP link.

5. **Monitoring** — `/health` endpoint, structured JSON logs, Sentry or similar for error tracking.

6. **Multi-instance** — sticky routing by room_id at load balancer; Postgres NOTIFY for cross-instance
   broadcast; or Redis pub/sub.

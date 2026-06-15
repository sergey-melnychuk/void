# VOID — Ephemeral Chat Rooms for Live Sessions

## Overview

VOID is a lightweight, ephemeral chat room service designed for conference talks, workshops, and live sessions. A presenter creates a room, shares a short link, and the room disappears when the session ends. No accounts, no persistence, no noise.

---

## Features

### Room Lifecycle

- **Create room** — single POST request, no account required
- **Optional password** — attendees must enter password to join; protects against randos with the link
- **TTL** — configurable at room creation (default: 2 hours, max: 24 hours); all state wiped on expiry
- **Manual close** — admin can close the room early
- **Write lock** — admin can freeze chat (temporarily or permanently); attendees see a "room is locked" indicator; useful during talk, unlocked for Q&A

### Links

- **Public link** — `void.sh/r/<room_id>` e.g. `void.sh/r/a3f9c2`; share on a slide; no auth required to join
- **Private link** — `void.sh/r/<room_id>/<secret>` e.g. `void.sh/r/a3f9c2/8f3kQpXmN2vLzR9w`; shown once at room creation; whoever holds it has admin privileges

### Users

- **Two roles only** — `admin` (holds admin token) and `user` (everyone else); no usernames, no per-user identity
- **No registration** — session token is ephemeral, scoped to the room, stored in a cookie for reconnect within TTL
- **Messages are anonymous** — chat shows only role badge: `[admin]` or `[user]`

### Chat

- **Real-time messaging** via WebSocket
- **Per-user rate limit** — configurable by admin at room creation (default: 1 message / 3 seconds)
- **Global write lock** — admin toggle; can be timed (e.g. locked for 5 minutes) or indefinite
- **Message moderation** — admin can delete individual messages
- **Reactions** — emoji reactions on messages (👍 ❤️ 😂 🔥 🤔); counts shown inline
- **Max message length** — 500 characters

### Q&A

- **Submit question** — any attendee can post a question to the Q&A board (separate from chat)
- **Upvoting** — one vote per user per question; sorted by vote count descending
- **Admin pins** — admin can pin a question to the top (currently being answered)
- **Admin dismiss** — admin can remove a question from the board

### Polls

- **Create poll** — admin only; multiple choice, 2–6 options
- **Live results** — results update in real-time as votes come in; displayed as bar chart (CSS only, no lib)
- **One vote per user** — enforced server-side per session token
- **Close poll** — admin closes voting; results frozen and displayed
- **Multiple polls** — admin can run polls sequentially; previous results remain visible

### Admin Controls (summary)

- Write lock (toggle / timed)
- Close room
- Delete messages
- Pin / dismiss Q&A questions
- Create / close polls
- View participant count

---

## Requirements

### Functional

- Room state (messages, Q&A, polls, participants) lives entirely in process memory; no external storage in v1
- Messages per room capped at N, configurable at room creation (default: 200, max: 1000); implemented as `VecDeque<Message>` — push back, pop front when full
- TTL enforced by a background `tokio::task` sweeping expired rooms periodically; no orphaned rooms
- Admin token validated server-side on every privileged WebSocket message; never stored, recomputed from private room segment
- Rate limiting enforced server-side per session token, not per IP
- Room ID: 3 bytes as hex, 6 lowercase hex chars (e.g. `a3f9c2`); 16M possible rooms
- Max participants per room: configurable at room creation; enforced at join

### Non-Functional

- Cold join (WebSocket connect + full room state snapshot) < 200ms
- Support 200 concurrent users per room without degradation
- No database, no Redis, no external dependencies in v1; single binary deployment
- Binary size target: < 20MB
- Svelte 5 + Vite build step; compiled output bundled into binary via `rust-embed`; no framework runtime shipped to browser

### Security

- **Public link** — 6-char hex room ID, e.g. `void.sh/r/a3f9c2`; safe to share on a slide
- **Private link** — room ID + 16-byte random secret, e.g. `void.sh/r/a3f9c2/8f3kQpXmN2vLzR9w`; whoever holds it is admin; shown once at room creation
- **Admin token** — derived client-side: `HMAC-SHA256(key=secret, msg=room_id)`; server recomputes and compares on every privileged action; never stored
- **Constant-time comparison** on admin token validation — no timing attacks
- Room password: bcrypt-hashed in memory
- No PII collected or stored
- HTTPS enforced in production; HTTP redirects to HTTPS
- CORS locked to own origin

---

## Tech Stack

| Layer | Choice | Notes |
|---|---|---|
| Backend | Rust / Axum | Async, pure JSON API + WebSocket, no HTML rendering |
| State | In-process `DashMap` | Room state in memory; `VecDeque` per room for capped message history |
| Broadcast | `tokio::sync::broadcast` | Per-room channel; no external pub/sub needed |
| Frontend | Svelte 5 + Vite | Compiled to vanilla JS, no runtime, minimal bundle |
| Styling | Vanilla CSS, custom properties | Dark theme, responsive, no framework |
| Static assets | `rust-embed` | HTML/JS/CSS compiled into binary, zero runtime deps |
| Config | ENV vars | 12-factor, no config files in repo |

### Frontend Architecture

Svelte 5 + Vite. Build output (`dist/`) is embedded into the Rust binary via `rust-embed` and served as static assets. No framework runtime shipped to the browser — Svelte compiles to vanilla JS.

**Build:**
```
frontend/
  src/
    App.svelte
    components/
      RoomCreate.svelte
      RoomJoin.svelte
      ChatPanel.svelte
      QABoard.svelte
      PollPanel.svelte
      AdminBar.svelte
      Reactions.svelte
    lib/
      store.js       # shared reactive state (Svelte stores)
      socket.js      # WebSocket lifecycle + event dispatch
  index.html
  vite.config.js
```

`cargo build` runs `vite build` as a `build.rs` step; output embedded via `rust-embed`.

**Component breakdown:**

| Component | Responsibility |
|---|---|
| `App` | Root; WebSocket lifecycle, global room state, routing (create / join / room) |
| `RoomCreate` | Room creation form: TTL, optional password, max participants, message history cap, rate limit |
| `RoomJoin` | Password entry, join confirmation |
| `ChatPanel` | Message list, input, write lock state |
| `QABoard` | Question list sorted by votes, submit form |
| `PollPanel` | Active poll display, voting, results bar |
| `AdminBar` | Write lock toggle, close room, participant count |
| `Reactions` | Emoji reaction strip per message |

**Reactive store (`src/lib/store.js`):**

```js
import { writable, derived } from 'svelte/store'

export const room = writable({
  id: null,
  locked: false,
  participants: 0,
  messages: [],
  questions: [],
  polls: [],
})
```

**WebSocket event routing (`src/lib/socket.js`):**

```js
ws.onmessage = ({ data }) => {
  const event = JSON.parse(data)
  room.update(r => {
    switch (event.type) {
      case 'message':            r.messages.push(event.payload); break
      case 'message_deleted':    r.messages = r.messages.filter(m => m.id !== event.payload.id); break
      case 'reaction':           /* update message reactions by id */ break
      case 'question':           r.questions.push(event.payload); break
      case 'vote':               /* update question vote count by id */ break
      case 'question_pinned':    /* set pinned flag by id */ break
      case 'question_dismissed': r.questions = r.questions.filter(q => q.id !== event.payload.question_id); break
      case 'poll_created':       r.polls.push(event.payload); break
      case 'poll_update':        /* update vote counts by id */ break
      case 'poll_closed':        /* set closed flag by id */ break
      case 'lock':               r.locked = event.payload.locked; break
      case 'room_closed':        window.location.href = '/'; break
      case 'participant_count':  r.participants = event.payload.count; break
    }
    return r
  })
}
```

**Alternative:** Vue 3 via CDN (`https://esm.sh/vue@3`) with Composition API is a viable drop-in alternative requiring no build step — trade slightly larger runtime (~33KB) for zero tooling. Recommended only if eliminating the build step is a hard requirement.

### UI/UX

- **Dark theme** — near-black background (`#0a0a0a`), off-white text, accent color: deep violet (`#7c3aed`) or void-appropriate cold blue
- **Layout** — three-column desktop (chat | Q&A | polls+admin), single-column mobile with tab switcher
- **Typography** — monospace for room codes and role badges; sans-serif for messages
- **Animations** — subtle fade-in for new messages and questions; no layout shifts
- **Write lock indicator** — full-width banner, hard to miss

---

## API Surface

All room interactions after join go through WebSocket. REST endpoints are minimal:

```
POST   /rooms                  Create room → { room_id, public_url, admin_url }
GET    /r/:room_id             Serve SPA (index.html from rust-embed)
GET    /r/:room_id/state       Initial room state snapshot (messages, questions, active poll, lock status)
WS     /r/:room_id/ws          WebSocket connection; query param: ?token=<session|admin>
GET    /assets/*               Static JS/CSS served from rust-embed
```

Room creation returns the private (admin) URL once — it is never retrievable again. The admin bookmarks it.

---

## WebSocket Protocol

All messages are JSON. Server broadcasts events to all room subscribers via `tokio::sync::broadcast`; Svelte store updates trigger reactive re-renders.

### Server → Client Events

| `type` | Payload | Description |
|---|---|---|
| `message` | `{ id, role: "admin"|"user", text, ts, reactions }` | New chat message |
| `message_deleted` | `{ id }` | Message removed by admin |
| `reaction` | `{ message_id, emoji, count }` | Reaction count updated |
| `question` | `{ id, text, votes, pinned }` | New Q&A question |
| `vote` | `{ question_id, votes }` | Question vote count updated |
| `question_pinned` | `{ question_id }` | Question pinned by admin |
| `question_dismissed` | `{ question_id }` | Question removed by admin |
| `poll_created` | `{ id, question, options }` | New poll opened |
| `poll_update` | `{ poll_id, options: [{ text, votes }] }` | Live vote counts |
| `poll_closed` | `{ poll_id }` | Poll closed, results final |
| `lock` | `{ locked: bool, until?: epoch_ms }` | Write lock state change |
| `room_closed` | `{}` | Room shut down by admin |
| `participant_count` | `{ count }` | Participant count update |
| `error` | `{ code, message }` | Rate limit, auth failure, etc. |

### Client → Server Messages

| `type` | Payload | Auth |
|---|---|---|
| `message` | `{ text }` | session token (cookie) |
| `reaction` | `{ message_id, emoji }` | session token |
| `question` | `{ text }` | session token |
| `vote` | `{ question_id }` | session token |
| `poll_vote` | `{ poll_id, option_index }` | session token |
| `admin_lock` | `{ locked: bool, duration_seconds?: u32 }` | admin token |
| `admin_delete_message` | `{ message_id }` | admin token |
| `admin_pin_question` | `{ question_id }` | admin token |
| `admin_dismiss_question` | `{ question_id }` | admin token |
| `admin_create_poll` | `{ question, options: [string] }` | admin token |
| `admin_close_poll` | `{ poll_id }` | admin token |
| `admin_close_room` | `{}` | admin token |

---

## Test Scenarios

### Room Creation

- Create room with no password → public link and admin link returned
- Create room with password → attendees without password cannot join
- Create room with TTL of 1 hour → room state gone after TTL
- Admin link with invalid token → 403
- Two rooms with same room ID cannot exist simultaneously (collision → retry with new random ID)

### Chat

- Message sent by user appears for all connected users in < 500ms
- User exceeding rate limit gets a transient error; message not sent
- Admin deletes message → disappears for all users in real-time
- Write lock enabled → chat input disabled for non-admin; admin can still post
- Timed write lock of 2 minutes → unlocks automatically; chat input re-enables
- Message > 500 chars → rejected with error
- User disconnects and reconnects within TTL → same session token (cookie), sees message history

### Q&A

- Question submitted → appears on Q&A board for all users
- Multiple users upvote same question → sorted to top in real-time
- User tries to vote twice → second vote rejected
- Admin pins question → appears at top regardless of vote count
- Admin dismisses question → removed from board for all users

### Polls

- Admin creates poll → appears for all users immediately
- User votes → results bar updates in real-time for all users
- User tries to vote twice → rejected
- Admin closes poll → voting disabled, final results shown
- Second poll created after first is closed → both visible, only new one accepts votes

### Reactions

- User reacts to message → count increments for all users
- Same user reacts again → toggles off (count decrements)

### Admin Controls

- Admin closes room → all connected users see "room closed" and are redirected
- Admin link opened in second browser → both sessions have admin privileges
- Admin token not present → admin actions return 403

### Load / Edge Cases

- 200 concurrent WebSocket connections to one room → all receive broadcasts
- Room with 0 connected users but within TTL → state preserved; next user to join sees history
- Server restart during active room → all in-memory state lost; acceptable for ephemeral rooms

---

## Deployment

### Environment Variables

```
HOST=0.0.0.0
PORT=8080
BASE_URL=https://void.sh
BCRYPT_COST=10
DEFAULT_TTL_SECONDS=7200            # 2 hours; max 86400 (24h)
MAX_TTL_SECONDS=86400
DEFAULT_MAX_MESSAGES=200
MAX_MESSAGES_PER_ROOM=1000
DEFAULT_MAX_PARTICIPANTS=100
MAX_PARTICIPANTS_PER_ROOM=500
DEFAULT_RATE_LIMIT_SECONDS=3        # min gap between messages per session
MAX_MESSAGE_LENGTH=500
TTL_SWEEP_INTERVAL_SECONDS=60       # how often background task reaps expired rooms
```

### Heroku

- `Procfile`: `web: ./void`
- Set env vars via `heroku config:set`
- Buildpack: `emk/heroku-buildpack-rust` or pre-build binary and use container stack
- WebSockets supported natively on Heroku; no special config needed
- **Note**: Heroku free dynos sleep after 30 min inactivity — use Basic dyno ($5/mo) for a live talk
- **Note**: in-memory state is lost on dyno restart; acceptable for ephemeral rooms

### Fly.io (recommended over Heroku)

- `fly.toml` with single `[http_service]` block; WebSockets work out of the box
- `fly deploy` builds via Dockerfile; no add-ons needed
- **Note**: in-memory state is lost on VM restart; acceptable for ephemeral rooms; single instance only (no horizontal scaling in v1)
- Free tier sufficient for conference use; persistent VM, no sleep
- Closest region selectable — important for latency during a live talk

### Render

- Docker-based deploy; no add-ons needed
- Free tier has cold starts — use paid ($7/mo) for live use

### Self-hosted (VPS)

- Single binary + systemd unit file
- Nginx reverse proxy for TLS termination:

```nginx
server {
    listen 443 ssl;
    server_name void.sh;

    ssl_certificate     /etc/letsencrypt/live/void.sh/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/void.sh/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";  # required for WebSocket
        proxy_set_header Host $host;
    }
}
```

### TLS / Let's Encrypt (self-hosted)

- Install Certbot: `apt install certbot python3-certbot-nginx`
- Issue cert: `certbot --nginx -d void.sh`
- Auto-renewal via systemd timer (installed by certbot automatically); verify with `systemctl status certbot.timer`
- Wildcard cert not needed unless running multi-tenant subdomains per room

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

## Persistence

### Goals

All room state survives server restarts within the room's TTL. Free and paid rooms
are treated identically — persistence is not a differentiator; longevity and custom
URLs are.

### Architecture

**Postgres (Neon)** + **SQLx**. Two tables only.

```sql
-- Room metadata: fast lookup without replaying events.
CREATE TABLE rooms (
  id                 TEXT PRIMARY KEY,
  title              TEXT,
  secret             TEXT NOT NULL,
  password_hash      TEXT,
  expires_at         BIGINT NOT NULL,   -- epoch ms
  created_at         BIGINT NOT NULL,
  max_messages       INT NOT NULL,
  max_participants   INT NOT NULL,
  rate_limit_ms      BIGINT NOT NULL,
  max_message_length INT NOT NULL,
  moderated          BOOLEAN NOT NULL DEFAULT FALSE
);

-- Append-only event log: one row per state-changing action.
CREATE TABLE events (
  seq      BIGSERIAL,
  room_id  TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  type     TEXT NOT NULL,
  payload  JSONB NOT NULL,
  ts       BIGINT NOT NULL   -- epoch ms
);

CREATE INDEX ON events (room_id, seq);   -- fast ordered replay per room
CREATE INDEX ON rooms (expires_at);      -- fast TTL sweep
```

### Event types

Storage payloads carry more data than broadcast payloads (e.g. `identity` for
dedup replay) but are never sent to clients.

| type | payload |
|---|---|
| `session_joined` | `{ token }` |
| `message_posted` | `{ id, role, text, ts, identity }` |
| `message_deleted` | `{ message_id }` |
| `reaction_toggled` | `{ message_id, emoji, identity }` |
| `pending_msg_posted` | `{ id, role, text, ts, identity }` |
| `pending_msg_approved` | `{ message_id }` |
| `pending_msg_rejected` | `{ message_id }` |
| `question_posted` | `{ id, text }` |
| `question_voted` | `{ question_id, identity }` |
| `question_pinned` | `{ question_id, pinned }` |
| `question_answered` | `{ question_id }` |
| `question_dismissed` | `{ question_id }` |
| `pending_q_posted` | `{ id, text }` |
| `pending_q_approved` | `{ question_id }` |
| `pending_q_rejected` | `{ question_id }` |
| `poll_created` | `{ id, question, options: [text] }` |
| `poll_voted` | `{ poll_id, option_idx, identity }` |
| `poll_closed` | `{ poll_id }` |
| `poll_deleted` | `{ poll_id }` |
| `room_lock_changed` | `{ locked }` |

No `room_created` event — the `rooms` row is the source of truth for metadata.
No `room_closed` event — `DELETE FROM rooms` cascades to events.

### Write strategy

The DB is **never in the broadcast hot path**:

1. Mutate in-memory state
2. Broadcast to WebSocket subscribers
3. Fire async DB write (no await before broadcast)

Clients never wait on Postgres. A crash between steps 2 and 3 loses at most the
last in-flight event — acceptable for a conference room tool.

Rate limiting state (`last_message_at`) is intentionally not persisted; it resets
on restart.

### Startup replay

```
SELECT * FROM rooms WHERE expires_at > now_ms();
```

For each room, replay its events in `seq` order to reconstruct the full
`RoomInner`: messages (with reactions), questions (votes, voter sets, pinned,
answered), polls (options, votes, voter sets), pending queues, sessions, lock
state.

`next_msg_id`, `next_question_id`, `next_poll_id` are derived from the max IDs
seen during replay.

Lock state: `effective_locked()` already checks `locked_until` against wall clock,
so a timed lock whose deadline passed during downtime auto-expires on first access.

### TTL sweep

```sql
DELETE FROM rooms WHERE expires_at < $1;
```

`ON DELETE CASCADE` removes all events for expired rooms. Sweep runs on the same
interval as the in-memory reap (`TTL_SWEEP_INTERVAL_SECONDS`).

### Scalability notes

The `(room_id, seq)` index makes replay queries touch only that room's rows —
functionally equivalent to table-per-room for read performance. At conference room
scale (thousands of rooms, hundreds of events each) Postgres handles this without
issue. Partitioning by time range is available if the table grows into the hundreds
of millions of rows.

## Future Work

- **Multi-instance scaling** — sticky sessions by room_id; Postgres NOTIFY or
  Redis pub/sub for cross-instance broadcast
- **Paid tier** — custom URL slugs, 30-day TTL, unlimited caps, recap export on
  expiry (JSON / PDF); Stripe + crypto payments
- **User accounts** — optional persistent identity across rooms (v3)
- **Mobile app** — React Native (v3)

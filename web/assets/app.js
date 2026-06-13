// VOID frontend — Vue 3, no build step. Loaded as a module; `vue` resolves via
// the import map in index.html. Implements the protocol described in VOID.md.
//
// Identity is a server-minted session token kept in localStorage (no cookies).
// It is carried to the server in the WebSocket subprotocol and an
// Authorization header — never in a URL — so it stays out of access logs.

import { createApp, reactive, computed, onMounted } from 'vue'

// ── Reactive room store (mirrors VOID.md store shape) ─────────────────────
const room = reactive({
  id: null,
  locked: false,
  lockedUntil: null,
  participants: 0,
  messages: [],
  questions: [],
  polls: [],
})

const REACTIONS = ['👍', '❤️', '😂', '🔥', '🤔']

// ── Helpers ───────────────────────────────────────────────────────────────
const sessionKey = (id) => `void_sess_${id}`

/** Derive the admin token: HMAC-SHA256(secret, room_id) as lowercase hex. */
async function deriveAdminToken(secret, roomId) {
  const enc = new TextEncoder()
  const key = await crypto.subtle.importKey(
    'raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
  )
  const sig = await crypto.subtle.sign('HMAC', key, enc.encode(roomId))
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, '0')).join('')
}

/** Append `item` only if no element with the same id is present (absorbs the
 *  snapshot/broadcast overlap without duplicating rows). */
function pushUnique(arr, item) {
  if (!arr.some((e) => e.id === item.id)) arr.push(item)
}

function applySnapshot(s) {
  room.id = s.id
  room.locked = s.locked
  room.lockedUntil = s.locked_until ?? null
  room.participants = s.participants
  room.messages = s.messages ?? []
  room.questions = s.questions ?? []
  room.polls = s.polls ?? []
}

// ── WebSocket lifecycle + server→client event routing ─────────────────────
let ws = null
let wsCtx = null              // { roomId, token } — kept for reconnect
let intentionalClose = false  // suppress reconnect after room_closed
let reconnectDelay = 1000
let stableTimer = null        // resets backoff only after a connection stays up
const ui = reactive({
  view: 'create',   // 'create' | 'created' | 'join' | 'room' | 'error'
  isAdmin: false,
  error: '',
  connected: false,
  toast: '',
  tab: 'chat',
  pollFormOpen: false,
})
let lastMessageDraft = '' // restored into the input if a send is rejected

function showToast(msg) {
  ui.toast = msg
  setTimeout(() => { if (ui.toast === msg) ui.toast = '' }, 4000)
}

function connect(roomId, token) {
  wsCtx = { roomId, token }
  const proto = location.protocol === 'https:' ? 'wss' : 'ws'
  // Token travels in the subprotocol, not the URL.
  ws = new WebSocket(`${proto}://${location.host}/r/${roomId}/ws`, [`void.token.${token}`])

  // Only reset the backoff once the connection has held for a few seconds —
  // otherwise a server that accepts then immediately closes (e.g. at the
  // participant cap) would turn the backoff into a ~1s hammer loop.
  ws.onopen = () => {
    ui.connected = true
    clearTimeout(stableTimer)
    stableTimer = setTimeout(() => { reconnectDelay = 1000 }, 3000)
  }

  ws.onmessage = ({ data }) => {
    const event = JSON.parse(data)
    const p = event.payload ?? {}
    switch (event.type) {
      case 'snapshot':           applySnapshot(p); break
      case 'message':            pushUnique(room.messages, p); break
      case 'message_deleted':    room.messages = room.messages.filter((m) => m.id !== p.id); break
      case 'reaction': {
        const m = room.messages.find((m) => m.id === p.message_id)
        if (m) { if (p.count > 0) m.reactions[p.emoji] = p.count; else delete m.reactions[p.emoji] }
        break
      }
      case 'question':           pushUnique(room.questions, p); break
      case 'vote': {
        const q = room.questions.find((q) => q.id === p.question_id)
        if (q) q.votes = p.votes
        break
      }
      case 'question_pinned':
        room.questions.forEach((q) => { q.pinned = p.pinned && q.id === p.question_id })
        break
      case 'question_dismissed':
        room.questions = room.questions.filter((q) => q.id !== p.question_id)
        break
      case 'poll_created':       pushUnique(room.polls, p); break
      case 'poll_update': {
        const poll = room.polls.find((x) => x.id === p.poll_id)
        if (poll) p.options.forEach((o, i) => { if (poll.options[i]) poll.options[i].votes = o.votes })
        break
      }
      case 'poll_closed': {
        const poll = room.polls.find((x) => x.id === p.poll_id)
        if (poll) poll.closed = true
        break
      }
      case 'lock':               room.locked = p.locked; room.lockedUntil = p.until ?? null; break
      case 'room_closed':
        intentionalClose = true
        if (ws) ws.close()
        ui.error = 'Room closed by the host.'
        ui.view = 'error'
        break
      case 'participant_count':  room.participants = p.count; break
      case 'error':
        showToast(p.message || p.code)
        // Restore an unsent chat message so the user doesn't lose their text —
        // but only if the input is empty, so we never clobber a newer draft.
        if (['rate_limited', 'too_long', 'locked'].includes(p.code) &&
            lastMessageDraft && draftRef && !draftRef.message) {
          draftRef.message = lastMessageDraft
        }
        break
    }
  }

  ws.onclose = () => {
    ui.connected = false
    clearTimeout(stableTimer)
    if (intentionalClose) return
    // Reconnect with backoff; the session token in localStorage is still valid
    // and the server replays state via a fresh snapshot on reconnect.
    setTimeout(() => { if (wsCtx) connect(wsCtx.roomId, wsCtx.token) }, reconnectDelay)
    reconnectDelay = Math.min(reconnectDelay * 2, 15000)
  }
}

function send(type, payload = {}) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, payload }))
    return true
  }
  showToast('Not connected — reconnecting…')
  return false
}

// draft model needs to be reachable from the socket error handler
let draftRef = null

// ── Root component ─────────────────────────────────────────────────────────
const App = {
  setup() {
    const form = reactive({ ttlHours: 2, password: '', maxParticipants: 100, maxMessages: 200, rateLimit: 3 })
    const created = reactive({ publicUrl: '', adminUrl: '', roomId: '', secret: '' })
    const joinForm = reactive({ password: '' })
    const draft = reactive({ message: '', question: '' })
    const pollForm = reactive({ question: '', options: ['', ''] })
    draftRef = draft

    // ── routing ──────────────────────────────────────────────────────────
    onMounted(async () => {
      const admin = location.pathname.match(/^\/r\/([0-9a-f]{6})\/([A-Za-z0-9]+)/)
      const user = location.pathname.match(/^\/r\/([0-9a-f]{6})\/?$/)
      if (admin) await enterAsAdmin(admin[1], admin[2])
      else if (user) await enterAsUser(user[1])
    })

    // ── create ─────────────────────────────────────────────────────────────
    async function createRoom() {
      const res = await fetch('/rooms', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          ttl_seconds: Math.round(form.ttlHours * 3600),
          password: form.password || null,
          max_participants: Number(form.maxParticipants),
          max_messages: Number(form.maxMessages),
          rate_limit_seconds: Number(form.rateLimit),
        }),
      })
      if (!res.ok) { ui.error = 'Could not create room'; ui.view = 'error'; return }
      const data = await res.json()
      created.publicUrl = data.public_url
      created.adminUrl = data.admin_url
      created.roomId = data.room_id
      created.secret = data.admin_url.split('/').pop()
      ui.view = 'created'
    }

    function enterRoom() {
      location.href = `/r/${created.roomId}/${created.secret}`
    }

    // ── join (user) ──────────────────────────────────────────────────────
    async function enterAsUser(roomId) {
      room.id = roomId
      const session = localStorage.getItem(sessionKey(roomId))
      const res = await fetch(`/r/${roomId}/join`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ session, password: joinForm.password || null }),
      })
      if (res.status === 403) { ui.view = 'join'; ui.error = joinForm.password ? 'Wrong password or room full' : ''; return }
      if (!res.ok) { ui.error = 'Room not found or expired'; ui.view = 'error'; return }
      const data = await res.json()
      localStorage.setItem(sessionKey(roomId), data.session_token)
      applySnapshot(data.state)
      ui.isAdmin = false
      ui.view = 'room'
      connect(roomId, data.session_token)
    }

    // ── enter (admin) ──────────────────────────────────────────────────────
    async function enterAsAdmin(roomId, secret) {
      room.id = roomId
      const token = await deriveAdminToken(secret, roomId)
      const res = await fetch(`/r/${roomId}/state`, { headers: { Authorization: `Bearer ${token}` } })
      if (res.status === 403) { ui.error = 'Invalid admin link (check the secret)'; ui.view = 'error'; return }
      if (!res.ok) { ui.error = 'Room not found or expired'; ui.view = 'error'; return }
      applySnapshot(await res.json())
      ui.isAdmin = true
      ui.view = 'room'
      connect(roomId, token)
    }

    // ── actions ──────────────────────────────────────────────────────────
    function sendMessage() {
      const text = draft.message.trim()
      if (!text) return
      lastMessageDraft = text
      if (send('message', { text })) draft.message = ''
    }
    function react(messageId, emoji) { send('reaction', { message_id: messageId, emoji }) }
    function submitQuestion() {
      const text = draft.question.trim()
      if (!text) return
      if (send('question', { text })) draft.question = ''
    }
    function voteQuestion(id) { send('vote', { question_id: id }) }
    function votePoll(pollId, idx) { send('poll_vote', { poll_id: pollId, option_index: idx }) }

    // admin
    function toggleLock() { send('admin_lock', { locked: !room.locked }) }
    function lockTimed(seconds) { send('admin_lock', { locked: true, duration_seconds: seconds }) }
    function deleteMessage(id) { send('admin_delete_message', { message_id: id }) }
    function pinQuestion(id) { send('admin_pin_question', { question_id: id }) }
    function dismissQuestion(id) { send('admin_dismiss_question', { question_id: id }) }
    function addPollOption() { if (pollForm.options.length < 6) pollForm.options.push('') }
    function removePollOption(i) { if (pollForm.options.length > 2) pollForm.options.splice(i, 1) }
    function createPoll() {
      const options = pollForm.options.map((o) => o.trim()).filter(Boolean)
      if (!pollForm.question.trim() || options.length < 2) return
      send('admin_create_poll', { question: pollForm.question.trim(), options })
      pollForm.question = ''
      pollForm.options = ['', '']
    }
    function closePoll(id) { send('admin_close_poll', { poll_id: id }) }
    function closeRoom() { if (confirm('Close the room for everyone?')) send('admin_close_room') }

    // ── derived ──────────────────────────────────────────────────────────
    const sortedQuestions = computed(() =>
      [...room.questions].sort((a, b) => (b.pinned - a.pinned) || (b.votes - a.votes)))
    const pollTotal = (poll) => poll.options.reduce((n, o) => n + o.votes, 0)
    const pct = (poll, o) => { const t = pollTotal(poll); return t ? Math.round((o.votes / t) * 100) : 0 }

    return {
      room, ui, form, created, joinForm, draft, pollForm, REACTIONS,
      createRoom, enterRoom, enterAsUser, sendMessage, react, submitQuestion,
      voteQuestion, votePoll, toggleLock, lockTimed, deleteMessage, pinQuestion,
      dismissQuestion, addPollOption, removePollOption, createPoll, closePoll, closeRoom,
      sortedQuestions, pollTotal, pct,
    }
  },

  template: `
  <!-- Create -->
  <div v-if="ui.view === 'create'" class="center">
    <div class="card">
      <h1>VOID</h1>
      <p style="color:var(--text-dim)">Ephemeral chat rooms for live sessions.</p>
      <div class="field"><label>TTL (hours, max 24)</label><input type="number" min="1" max="24" v-model="form.ttlHours" /></div>
      <div class="field"><label>Password (optional)</label><input type="password" v-model="form.password" /></div>
      <div class="field"><label>Max participants</label><input type="number" v-model="form.maxParticipants" /></div>
      <div class="field"><label>Message history cap</label><input type="number" v-model="form.maxMessages" /></div>
      <div class="field"><label>Rate limit (seconds between messages)</label><input type="number" v-model="form.rateLimit" /></div>
      <button @click="createRoom">Create room</button>
    </div>
  </div>

  <!-- Created — show links once -->
  <div v-else-if="ui.view === 'created'" class="center">
    <div class="card">
      <h1>Room ready</h1>
      <div class="field">
        <label>Public link (share on a slide)</label>
        <input readonly :value="created.publicUrl" @focus="$event.target.select()" />
      </div>
      <div class="field">
        <label>Admin link — shown once, save it now</label>
        <input readonly :value="created.adminUrl" @focus="$event.target.select()" />
      </div>
      <button @click="enterRoom">Enter as admin</button>
    </div>
  </div>

  <!-- Join (password) -->
  <div v-else-if="ui.view === 'join'" class="center">
    <div class="card">
      <h1>Join room</h1>
      <p v-if="ui.error" style="color:var(--danger)">{{ ui.error }}</p>
      <div class="field"><label>Password</label><input type="password" v-model="joinForm.password" @keyup.enter="enterAsUser(room.id)" /></div>
      <button @click="enterAsUser(room.id)">Enter room</button>
    </div>
  </div>

  <!-- Error -->
  <div v-else-if="ui.view === 'error'" class="center">
    <div class="card"><h1>VOID</h1><p style="color:var(--danger)">{{ ui.error }}</p></div>
  </div>

  <!-- Room -->
  <div v-else class="room">
    <header>
      <span class="brand">VOID</span>
      <span class="room-code">{{ room.id }}</span>
      <span class="badge admin" v-if="ui.isAdmin">[admin]</span>
      <span class="spacer"></span>
      <span class="participants">{{ room.participants }} online</span>
    </header>

    <div class="banner-row">
      <div v-if="!ui.connected" class="lock-banner" style="background:var(--danger)">⚠ Reconnecting…</div>
      <div v-else-if="room.locked" class="lock-banner">🔒 Room is locked — chat is read-only</div>
    </div>

    <nav class="tabs">
      <button :class="{active: ui.tab==='chat'}" @click="ui.tab='chat'">Chat</button>
      <button :class="{active: ui.tab==='qa'}" @click="ui.tab='qa'">Q&amp;A</button>
      <button :class="{active: ui.tab==='polls'}" @click="ui.tab='polls'">Polls</button>
    </nav>

    <!-- Chat -->
    <section class="panel" :class="{'hidden-mobile': ui.tab!=='chat'}">
      <h2>Chat</h2>
      <div class="panel-body">
        <div v-for="m in room.messages" :key="m.id" class="msg">
          <span class="badge" :class="m.role">[{{ m.role }}]</span>{{ m.text }}
          <button v-if="ui.isAdmin" class="ghost" style="padding:0 .3rem;font-size:.7rem" @click="deleteMessage(m.id)">✕</button>
          <div style="margin-top:.2rem">
            <button v-for="e in REACTIONS" :key="e" class="ghost" style="padding:0 .3rem" @click="react(m.id, e)">
              {{ e }}<span v-if="m.reactions[e]" style="color:var(--text-dim)"> {{ m.reactions[e] }}</span>
            </button>
          </div>
        </div>
      </div>
      <div class="panel-footer">
        <input placeholder="Message…" maxlength="500"
               :disabled="room.locked && !ui.isAdmin" v-model="draft.message" @keyup.enter="sendMessage" />
      </div>
    </section>

    <!-- Q&A -->
    <section class="panel" :class="{'hidden-mobile': ui.tab!=='qa'}">
      <h2>Q&amp;A</h2>
      <div class="panel-body">
        <div v-for="q in sortedQuestions" :key="q.id" class="msg">
          <button class="ghost" style="padding:0 .4rem" :disabled="room.locked && !ui.isAdmin" @click="voteQuestion(q.id)">▲ {{ q.votes }}</button>
          <span v-if="q.pinned" style="color:var(--accent)">📌 </span>{{ q.text }}
          <template v-if="ui.isAdmin">
            <button class="ghost" style="padding:0 .3rem;font-size:.7rem" @click="pinQuestion(q.id)">pin</button>
            <button class="ghost" style="padding:0 .3rem;font-size:.7rem" @click="dismissQuestion(q.id)">✕</button>
          </template>
        </div>
      </div>
      <div class="panel-footer">
        <input placeholder="Ask a question…" maxlength="500"
               :disabled="room.locked && !ui.isAdmin" v-model="draft.question" @keyup.enter="submitQuestion" />
      </div>
    </section>

    <!-- Polls + Admin -->
    <section class="panel" :class="{'hidden-mobile': ui.tab!=='polls'}">
      <h2>Polls &amp; Admin</h2>
      <div class="panel-body">
        <!-- Admin: create poll (collapsible, always on top) -->
        <div v-if="ui.isAdmin" class="poll-form-wrap">
          <button class="ghost poll-form-toggle" @click="ui.pollFormOpen = !ui.pollFormOpen">
            New poll <span>{{ ui.pollFormOpen ? '▲' : '▼' }}</span>
          </button>
          <div v-if="ui.pollFormOpen" class="poll-form-body">
            <div class="field"><input placeholder="Question" v-model="pollForm.question" /></div>
            <div class="field" v-for="(o, i) in pollForm.options" :key="i" style="flex-direction:row;gap:.3rem">
              <input style="flex:1" :placeholder="'Option ' + (i+1)" v-model="pollForm.options[i]" />
              <button class="ghost" @click="removePollOption(i)" :disabled="pollForm.options.length<=2">−</button>
            </div>
            <button class="ghost" @click="addPollOption" :disabled="pollForm.options.length>=6">+ option</button>
            <button @click="createPoll" style="margin-left:.4rem">Create poll</button>
          </div>
        </div>

        <div v-for="p in [...room.polls].reverse()" :key="p.id" class="msg">
          <strong>{{ p.question }}</strong>
          <span v-if="p.closed" style="color:var(--text-dim)"> · closed</span>
          <div v-for="(o, i) in p.options" :key="i" style="margin:.3rem 0">
            <button class="ghost" style="width:100%;text-align:left" :disabled="p.closed || (room.locked && !ui.isAdmin)" @click="votePoll(p.id, i)">
              {{ o.text }} — {{ o.votes }} ({{ pct(p, o) }}%)
            </button>
            <div style="height:4px;background:var(--accent);border-radius:2px" :style="{width: pct(p, o) + '%'}"></div>
          </div>
          <button v-if="ui.isAdmin && !p.closed" class="ghost" @click="closePoll(p.id)">Close poll</button>
        </div>
      </div>

      <!-- Admin controls -->
      <div v-if="ui.isAdmin" class="panel-footer">
        <button class="ghost admin-btn" @click="toggleLock">{{ room.locked ? 'Unlock' : 'Lock' }}</button>
        <button class="ghost admin-btn" style="color:var(--danger);border-color:var(--danger)" @click="closeRoom">Close room</button>
      </div>
    </section>

    <!-- Transient toast for server errors (rate limit, locked, etc.) -->
    <div v-if="ui.toast" class="toast">{{ ui.toast }}</div>
  </div>
  `,
}

createApp(App).mount('#app')

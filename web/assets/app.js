// VOID frontend — Vue 3, no build step. Loaded as a module; `vue` resolves via
// the import map in index.html. Implements the protocol described in VOID.md.
//
// Identity is a server-minted session token kept in localStorage (no cookies).
// It is carried to the server in the WebSocket subprotocol and an
// Authorization header — never in a URL — so it stays out of access logs.

import { createApp, reactive, computed, onMounted, nextTick, watch } from 'vue'
import QRCode from 'qrcode'

// ── Reactive room store (mirrors VOID.md store shape) ─────────────────────
const room = reactive({
  id: null,
  title: '',
  locked: false,
  lockedUntil: null,
  participants: 0,
  messages: [],
  questions: [],
  polls: [],
  pendingMessages: [],
  pendingQuestions: [],
})

// Locally-submitted items awaiting moderation: { id, text, status: 'pending'|'rejected' }
const myPending = reactive({ messages: [], questions: [] })
const myVotes = reactive(new Set())

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
  room.title = s.title ?? ''
  room.locked = s.locked
  room.lockedUntil = s.locked_until ?? null
  room.participants = s.participants
  room.messages = s.messages ?? []
  room.questions = s.questions ?? []
  room.polls = s.polls ?? []
  room.pendingMessages = s.pending_messages ?? []
  room.pendingQuestions = s.pending_questions ?? []
  myPending.messages = []
  myPending.questions = []
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
  pollModalOpen: false,
  qrOpen: false,
  isDisplay: false,
  displayMode: 'questions',
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
      case 'message':
        pushUnique(room.messages, p)
        room.pendingMessages = room.pendingMessages.filter((m) => m.id !== p.id)
        myPending.messages = myPending.messages.filter((m) => m.id !== p.id)
        break
      case 'message_deleted':    room.messages = room.messages.filter((m) => m.id !== p.id); break
      case 'reaction': {
        const m = room.messages.find((m) => m.id === p.message_id)
        if (m) { if (p.count > 0) m.reactions[p.emoji] = p.count; else delete m.reactions[p.emoji] }
        break
      }
      case 'question':
        pushUnique(room.questions, p)
        room.pendingQuestions = room.pendingQuestions.filter((q) => q.id !== p.id)
        myPending.questions = myPending.questions.filter((q) => q.id !== p.id)
        break
      case 'vote': {
        const q = room.questions.find((q) => q.id === p.question_id)
        if (q) q.votes = p.votes
        break
      }
      case 'question_pinned':
        room.questions.forEach((q) => { q.pinned = p.pinned && q.id === p.question_id })
        break
      case 'question_answered': {
        const q = room.questions.find((q) => q.id === p.question_id)
        if (q) { q.answered = true; q.pinned = false }
        break
      }
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
      case 'poll_deleted':
        room.polls = room.polls.filter((x) => x.id !== p.poll_id)
        break
      case 'pending_message':
        if (ui.isAdmin) pushUnique(room.pendingMessages, p)
        else if (!myPending.messages.some((m) => m.id === p.id))
          myPending.messages.push({ id: p.id, text: p.text, status: 'pending' })
        break
      case 'pending_message_rejected':
        room.pendingMessages = room.pendingMessages.filter((m) => m.id !== p.id)
        { const m = myPending.messages.find((m) => m.id === p.id); if (m) m.status = 'rejected' }
        break
      case 'pending_question':
        if (ui.isAdmin) pushUnique(room.pendingQuestions, p)
        else if (!myPending.questions.some((q) => q.id === p.id))
          myPending.questions.push({ id: p.id, text: p.text, status: 'pending' })
        break
      case 'pending_question_rejected':
        room.pendingQuestions = room.pendingQuestions.filter((q) => q.id !== p.id)
        { const q = myPending.questions.find((q) => q.id === p.id); if (q) q.status = 'rejected' }
        break
      case 'display_mode':       ui.displayMode = p.mode; break
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
    const form = reactive({ ttlHours: 2, title: '', password: '', maxParticipants: 100, maxMessages: 200, rateLimit: 3, moderated: false })
    const created = reactive({ publicUrl: '', adminUrl: '', roomId: '', secret: '' })
    const joinForm = reactive({ password: '' })
    const draft = reactive({ message: '', question: '' })
    const pollForm = reactive({ question: '', options: ['', ''] })
    draftRef = draft

    // ── routing ──────────────────────────────────────────────────────────
    onMounted(async () => {
      const display = location.pathname.match(/^\/w\/([A-Za-z0-9]+)\/?$/)
      const admin = location.pathname.match(/^\/r\/([A-Za-z0-9]+)\/([A-Za-z0-9]+)$/)
      const user = location.pathname.match(/^\/r\/([A-Za-z0-9]+)\/?$/)
      if (display) await enterAsDisplay(display[1])
      else if (admin) await enterAsAdmin(admin[1], admin[2])
      else if (user) await enterAsUser(user[1])
    })

    // ── create ─────────────────────────────────────────────────────────────
    async function createRoom() {
      const res = await fetch('/rooms', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          ttl_seconds: Math.round(form.ttlHours * 3600),
          title: form.title.trim() || null,
          password: form.password || null,
          max_participants: Number(form.maxParticipants),
          max_messages: Number(form.maxMessages),
          rate_limit_seconds: Number(form.rateLimit),
          moderated: form.moderated,
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

    // ── enter (display) ────────────────────────────────────────────────────
    async function enterAsDisplay(roomId) {
      room.id = roomId
      const session = localStorage.getItem(sessionKey(roomId))
      const res = await fetch(`/r/${roomId}/join`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ session, password: null, display: true }),
      })
      if (!res.ok) { ui.error = 'Room not found or expired'; ui.view = 'error'; return }
      const data = await res.json()
      localStorage.setItem(sessionKey(roomId), data.session_token)
      applySnapshot(data.state)
      ui.isAdmin = false
      ui.isDisplay = true
      ui.view = 'room'
      connect(roomId, data.session_token)
      generateDisplayQR()
      document.addEventListener('keydown', (e) => {
        if (e.key === 'f' || e.key === 'F') {
          if (!document.fullscreenElement) document.documentElement.requestFullscreen()
          else document.exitFullscreen()
        }
      })
    }

    // ── enter (admin) ──────────────────────────────────────────────────────
    async function enterAsAdmin(roomId, secret) {
      created.secret = secret
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
    function react(messageId, emoji) { if (!ui.isDisplay) send('reaction', { message_id: messageId, emoji }) }
    function submitQuestion() {
      const text = draft.question.trim()
      if (!text) return
      if (send('question', { text })) draft.question = ''
    }
    function voteQuestion(id) {
      if (ui.isDisplay) return
      if (!send('vote', { question_id: id })) return
      if (myVotes.has(id)) myVotes.delete(id); else myVotes.add(id)
    }
    function votePoll(pollId, idx) { send('poll_vote', { poll_id: pollId, option_index: idx }) }

    // admin
    function toggleLock() { send('admin_lock', { locked: !room.locked }) }
    function lockTimed(seconds) { send('admin_lock', { locked: true, duration_seconds: seconds }) }
    function deleteMessage(id) { send('admin_delete_message', { message_id: id }) }
    function pinQuestion(id) { send('admin_pin_question', { question_id: id }) }
    function dismissQuestion(id) { send('admin_dismiss_question', { question_id: id }) }
    function answerQuestion(id) { send('admin_answer_question', { question_id: id }) }
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
    function deletePoll(id) { send('admin_delete_poll', { poll_id: id }) }
    function approveMessage(id) { send('admin_approve_message', { message_id: id }) }
    function rejectMessage(id) { if (confirm('Drop this message?')) send('admin_reject_message', { message_id: id }) }
    function approveQuestion(id) { send('admin_approve_question', { question_id: id }) }
    function rejectQuestion(id) { if (confirm('Drop this question?')) send('admin_reject_question', { question_id: id }) }
    function closeRoom() { if (confirm('Close the room for everyone?')) send('admin_close_room') }

    function setDisplayMode(mode) { send('admin_display_mode', { mode }) }

    function generateDisplayQR() {
      nextTick(() => {
        const canvas = document.getElementById('display-qr-canvas')
        if (!canvas) return
        const url = `${location.protocol}//${location.host}/r/${room.id}`
        QRCode.toCanvas(canvas, url, { width: 400, margin: 2 })
      })
    }

    function showDisplay() {
      window.open(`/w/${room.id}`, '_blank', 'noopener,width=1280,height=800')
    }

    function showQR() {
      ui.qrOpen = true
      if (!ui.isDisplay) history.replaceState(null, '', `/r/${room.id}`)
      nextTick(() => {
        const canvas = document.getElementById('qr-canvas')
        if (!canvas) return
        const url = `${location.protocol}//${location.host}/r/${room.id}`
        QRCode.toCanvas(canvas, url, { width: Math.min(window.innerWidth, window.innerHeight) * 0.7, margin: 2 })
      })
    }

    function hideQR() {
      ui.qrOpen = false
      if (ui.isAdmin) history.replaceState(null, '', `/r/${room.id}/${created.secret}`)
    }

    // ── derived ──────────────────────────────────────────────────────────
    const sortedQuestions = computed(() =>
      [...room.questions].sort((a, b) =>
        (a.answered - b.answered) || (b.pinned - a.pinned) || (b.votes - a.votes)))
    const pollTotal = (poll) => poll.options.reduce((n, o) => n + o.votes, 0)
    const pct = (poll, o) => { const t = pollTotal(poll); return t ? Math.round((o.votes / t) * 100) : 0 }

    return {
      room, ui, myPending, myVotes, form, created, joinForm, draft, pollForm, REACTIONS, location,
      createRoom, enterRoom, enterAsUser, sendMessage, react, submitQuestion,
      voteQuestion, votePoll, toggleLock, lockTimed, deleteMessage, pinQuestion,
      dismissQuestion, answerQuestion, addPollOption, removePollOption, createPoll, closePoll, deletePoll, closeRoom,
      approveMessage, rejectMessage, approveQuestion, rejectQuestion, showQR, hideQR, showDisplay, setDisplayMode,
      sortedQuestions, pollTotal, pct,
    }
  },

  template: `
  <!-- Create -->
  <div v-if="ui.view === 'create'" class="center">
    <div class="card">
      <h1>VOID</h1>
      <p style="color:var(--text-dim)">Ephemeral chat rooms for live sessions.</p>
      <div class="field"><label>Title (optional, max 20 chars)</label><input maxlength="20" v-model="form.title" placeholder="e.g. RustMeet 2026" /></div>
      <div class="field"><label>TTL (hours, max 24)</label><input type="number" min="1" max="24" v-model="form.ttlHours" /></div>
      <div class="field"><label>Password (optional)</label><input type="password" v-model="form.password" /></div>
      <div class="field"><label>Max participants</label><input type="number" v-model="form.maxParticipants" /></div>
      <div class="field"><label>Message history cap</label><input type="number" v-model="form.maxMessages" /></div>
      <div class="field"><label>Rate limit (seconds between messages)</label><input type="number" v-model="form.rateLimit" /></div>
      <div class="field" style="flex-direction:row;align-items:center;gap:.5rem">
        <input type="checkbox" id="mod" v-model="form.moderated" style="width:auto" />
        <label for="mod" style="font-size:1rem;color:var(--text)">
          <span class="tooltip-anchor">Moderated<span class="tooltip-pop">Admin approves every message &amp; question before it's visible to others</span></span>
        </label>
      </div>
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
      <span class="room-code" @click="showQR" style="cursor:pointer" title="Show QR code">{{ room.id }}</span>
      <span v-if="room.title" class="room-title">{{ room.title }}</span>
      <span class="badge admin" v-if="ui.isAdmin && !ui.isDisplay">[admin]</span>
      <span class="spacer"></span>
      <template v-if="ui.isAdmin && !ui.isDisplay">
        <button class="ghost header-btn desktop-only" @click="showDisplay">Show</button>
        <button class="ghost header-btn desktop-only" @click="ui.pollModalOpen = true">Poll</button>
        <button class="ghost header-btn desktop-only lock-btn" @click="toggleLock">{{ room.locked ? 'Unlock' : 'Lock' }}</button>
        <button class="ghost header-btn desktop-only" style="color:var(--danger);border-color:var(--danger)" @click="closeRoom">Close</button>
      </template>
      <span class="participants">{{ room.participants }} online</span>
    </header>

    <div class="banner-row">
      <div v-if="!ui.connected" class="lock-banner" style="background:var(--danger)">⚠ Reconnecting…</div>
      <div v-else-if="room.locked" class="lock-banner">🔒 Room is locked — chat is read-only</div>
    </div>

    <div v-if="ui.isAdmin && !ui.isDisplay" class="admin-bar">
      <button class="ghost header-btn" @click="showDisplay">Show</button>
      <button class="ghost header-btn" @click="ui.pollModalOpen = true">Poll</button>
      <button class="ghost header-btn lock-btn" @click="toggleLock">{{ room.locked ? 'Unlock' : 'Lock' }}</button>
      <button class="ghost header-btn" style="color:var(--danger);border-color:var(--danger)" @click="closeRoom">Close</button>
    </div>

    <nav class="tabs">
      <button :class="{active: ui.tab==='chat'}" @click="ui.tab='chat'">Messages</button>
      <button :class="{active: ui.tab==='qa'}" @click="ui.tab='qa'">Questions</button>
      <button :class="{active: ui.tab==='polls'}" @click="ui.tab='polls'">Polls</button>
    </nav>

    <!-- Messages -->
    <section class="panel" :class="{'hidden-mobile': ui.tab!=='chat'}">
      <h2>Messages <span v-if="room.messages.length" class="panel-count">{{ room.messages.length }}</span></h2>
      <div class="panel-body">
        <div v-if="ui.isAdmin && !ui.isDisplay && room.pendingMessages.length" class="pending-queue">
          <div class="pending-label">Pending approval</div>
          <div v-for="m in room.pendingMessages" :key="m.id" class="msg pending-item">
            <span class="badge user">[user]</span>{{ m.text }}
            <div style="margin-top:.3rem;display:flex;gap:.4rem">
              <button class="ghost" style="color:var(--accent);border-color:var(--accent)" @click="approveMessage(m.id)">Approve</button>
              <button class="ghost" style="color:var(--danger);border-color:var(--danger)" @click="rejectMessage(m.id)">Drop</button>
            </div>
          </div>
        </div>
        <div v-for="m in room.messages" :key="m.id" class="msg">
          <span class="badge" :class="m.role">[{{ m.role }}]</span>{{ m.text }}
          <button v-if="ui.isAdmin && !ui.isDisplay" class="ghost" style="padding:0 .3rem;font-size:.7rem" @click="deleteMessage(m.id)">✕</button>
          <div style="margin-top:.2rem;display:flex;flex-wrap:wrap;gap:.25rem;align-items:center">
            <button v-for="e in REACTIONS.filter(e => m.reactions[e])" :key="e" class="ghost reaction"
              :disabled="ui.isDisplay" @click="react(m.id, e)">
              {{ e }} {{ m.reactions[e] }}
            </button>
            <div v-if="!ui.isDisplay" class="reaction-picker" style="position:relative;display:inline-block">
              <button class="ghost reaction add-reaction" @click="m._pick=!m._pick">+</button>
              <div v-if="m._pick" class="reaction-menu">
                <button v-for="e in REACTIONS" :key="e" class="ghost reaction" @click="react(m.id, e);m._pick=false">{{ e }}</button>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div v-if="!ui.isAdmin && !ui.isDisplay && myPending.messages.length" class="my-pending-wrap">
        <div v-for="m in myPending.messages" :key="m.id" class="my-pending-item" :class="m.status">
          {{ m.text }}
          <span class="my-pending-status">{{ m.status === 'rejected' ? 'dropped' : 'awaiting approval…' }}</span>
        </div>
      </div>
      <div v-if="!ui.isDisplay" class="panel-footer">
        <textarea placeholder="Message…" maxlength="500" rows="3" class="msg-input"
               :disabled="room.locked && !ui.isAdmin" v-model="draft.message"
               @keydown.enter.exact.prevent="sendMessage"></textarea>
      </div>
    </section>

    <!-- Questions -->
    <section class="panel" :class="{'hidden-mobile': ui.tab!=='qa'}">
      <h2>Questions <span v-if="room.questions.length" class="panel-count">{{ room.questions.length }}</span></h2>
      <div class="panel-body">
        <div v-if="ui.isAdmin && !ui.isDisplay && room.pendingQuestions.length" class="pending-queue">
          <div class="pending-label">Pending approval</div>
          <div v-for="q in room.pendingQuestions" :key="q.id" class="msg pending-item">
            {{ q.text }}
            <div style="margin-top:.3rem;display:flex;gap:.4rem">
              <button class="ghost" style="color:var(--accent);border-color:var(--accent)" @click="approveQuestion(q.id)">Approve</button>
              <button class="ghost" style="color:var(--danger);border-color:var(--danger)" @click="rejectQuestion(q.id)">Drop</button>
            </div>
          </div>
        </div>
        <div v-for="q in sortedQuestions" :key="q.id" class="msg" :class="{'question-answered': q.answered}">
          <button v-if="!ui.isDisplay" class="ghost" :class="{'voted-btn': myVotes.has(q.id)}" style="padding:0 .4rem" :disabled="q.answered || (room.locked && !ui.isAdmin)" @click="voteQuestion(q.id)">▲ {{ q.votes }}</button>
          <span v-else style="color:var(--text-dim);font-size:.85rem;padding:0 .3rem">▲ {{ q.votes }}</span>
          <span v-if="q.pinned" style="color:var(--accent)">📌 </span>{{ q.text }}
          <template v-if="ui.isAdmin && !ui.isDisplay">
            <button v-if="!q.answered" class="ghost" style="padding:0 .3rem;font-size:.7rem" @click="answerQuestion(q.id)">✓</button>
            <button v-if="!q.answered" class="ghost" style="padding:0 .3rem;font-size:.7rem" @click="pinQuestion(q.id)">pin</button>
            <button class="ghost" style="padding:0 .3rem;font-size:.7rem" @click="dismissQuestion(q.id)">✕</button>
          </template>
        </div>
      </div>
      <div v-if="!ui.isAdmin && !ui.isDisplay && myPending.questions.length" class="my-pending-wrap">
        <div v-for="q in myPending.questions" :key="q.id" class="my-pending-item" :class="q.status">
          {{ q.text }}
          <span class="my-pending-status">{{ q.status === 'rejected' ? 'dropped' : 'awaiting approval…' }}</span>
        </div>
      </div>
      <div v-if="!ui.isDisplay" class="panel-footer">
        <textarea placeholder="Ask a question…" maxlength="500" rows="3" class="msg-input"
               :disabled="room.locked && !ui.isAdmin" v-model="draft.question"
               @keydown.enter.exact.prevent="submitQuestion"></textarea>
      </div>
    </section>

    <!-- Polls -->
    <section class="panel" :class="{'hidden-mobile': ui.tab!=='polls'}">
      <h2>Polls <span v-if="room.polls.length" class="panel-count">{{ room.polls.length }}</span></h2>
      <div class="panel-body">
        <div v-for="p in [...room.polls].reverse()" :key="p.id" class="msg">
          <strong>{{ p.question }}</strong>
          <span v-if="p.closed" style="color:var(--text-dim)"> · closed</span>
          <div v-for="(o, i) in p.options" :key="i" style="margin:.3rem 0">
            <button class="ghost" style="width:100%;text-align:left" :disabled="p.closed || (room.locked && !ui.isAdmin)" @click="votePoll(p.id, i)">
              {{ o.text }} — {{ o.votes }} ({{ pct(p, o) }}%)
            </button>
            <div style="height:4px;background:var(--accent);border-radius:2px" :style="{width: pct(p, o) + '%'}"></div>
          </div>
          <button v-if="ui.isAdmin && !ui.isDisplay && !p.closed" class="ghost" @click="closePoll(p.id)">Close poll</button>
          <button v-if="ui.isAdmin && !ui.isDisplay" class="ghost" style="color:var(--danger);border-color:var(--danger)" @click="deletePoll(p.id)">Delete</button>
        </div>
      </div>
    </section>

    <!-- Poll creation modal -->
    <div v-if="ui.pollModalOpen" class="modal-overlay" @click.self="ui.pollModalOpen = false">
      <div class="modal-card">
        <h2 style="margin-top:0">New poll</h2>
        <div class="field"><input placeholder="Question" v-model="pollForm.question" /></div>
        <div class="field" v-for="(o, i) in pollForm.options" :key="i" style="flex-direction:row;gap:.3rem">
          <input style="flex:1" :placeholder="'Option ' + (i+1)" v-model="pollForm.options[i]" />
          <button class="ghost" @click="removePollOption(i)" :disabled="pollForm.options.length<=2">−</button>
        </div>
        <div style="display:flex;gap:.5rem;margin-top:.5rem">
          <button class="ghost" @click="addPollOption" :disabled="pollForm.options.length>=6">+ option</button>
          <button @click="createPoll();ui.pollModalOpen=false" style="margin-left:auto">Create poll</button>
          <button class="ghost" @click="ui.pollModalOpen=false">Cancel</button>
        </div>
      </div>
    </div>

    <!-- Transient toast for server errors (rate limit, locked, etc.) -->
    <div v-if="ui.toast" class="toast">{{ ui.toast }}</div>

    <!-- QR overlay — click anywhere to dismiss -->
    <div v-if="ui.qrOpen" class="qr-overlay" @click="hideQR()">
      <canvas id="qr-canvas"></canvas>
      <div class="qr-url">{{ location.host }}/r/{{ room.id }}</div>
      <div class="qr-hint">tap anywhere to close</div>
    </div>

    <!-- Display view: persistent QR widget bottom-right -->
    <div v-if="ui.isDisplay" class="display-qr-widget">
      <canvas id="display-qr-canvas"></canvas>
      <div class="display-qr-url">{{ location.host }}/r/{{ room.id }}</div>
    </div>
  </div>
  `,
}

createApp(App).mount('#app')

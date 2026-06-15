# VOID — Free vs Paid

## Free

- **TTL**: up to 3 hours
- **URL**: random hex ID (e.g. `0xff.wtf/r/4f2a1c`)
- **Participants**: max 20
- **Messages**: max 100
- **Questions**: max 10
- **Polls**: max 1
- **Persistence**: survives server restarts within TTL
- **Recap**: none
- **Ownership**: admin URL is the only credential — loss means permanent lockout
- **Topics**: admin can define topics; users subscribe to one topic at a time
- **Price**: free

## Paid

- **TTL**: up to 30 days, then room is closed and recap delivered
- **URL**: custom slug (e.g. `0xff.wtf/r/rustconf`), verified by email domain + manual approval
- **Participants**: unlimited
- **Messages**: unlimited
- **Questions**: unlimited
- **Polls**: unlimited
- **Persistence**: survives server restarts within TTL
- **Recap**: full room data export (JSON or PDF) on expiry
- **Ownership**: email on file (from payment); admin URL delivered by email; secret reset available if URL lost
- **Topics**: admin can define topics; users can subscribe to multiple topics simultaneously
- **Price**: $20/day/room, $50/day for custom slug

## Open Design Questions (paid tier)

- **Custom slug / squatters** — email OTP proves domain ownership (e.g. `*@eurorust.eu` to claim
  `eurorust2026`), but company names (`stripe`) need manual review as a second gate since any
  employee could claim it. OTP alone is not enough.

- **Multi-admin** — current model is one admin secret. Paid rooms likely need a team (conference
  has multiple organizers). Options: share the secret, or let the primary admin mint secondary
  tokens. Not yet decided.

- **Recap on early close** — recap is planned on TTL expiry, but if admin closes the room early,
  does recap deliver immediately or wait for the original expiry date? Immediate delivery is
  probably more useful.

- **TTL extension** — can a paid room extend its TTL by paying more? Current model bakes
  `expires_at` at creation. Extension needs an `admin_extend_ttl` action + DB update + payment
  flow. Not yet built.

- **Topics + moderation** — in a moderated room with topics, the pending queue should be
  filterable by topic; otherwise at conference scale the admin queue becomes unmanageable.

- **Free → paid upgrade** — can an existing free room be upgraded mid-session, or must you
  always start a new paid room? Starting fresh is simpler; mid-session upgrade avoids losing
  history but requires a payment gate on an existing room_id.

## Both

- Moderated mode (admin approves messages and questions before broadcast)
- Room lock (read-only mode)
- Question voting, pinning, answering
- Poll creation and closing
- Public display window (`0xff.wtf/w/<id>`)
- Admin display controls (Show button, fullscreen on F)
- Reactions on messages
- Real-time presence counter
- Ephemeral by design — no accounts required

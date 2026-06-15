CREATE TABLE IF NOT EXISTS rooms (
    id                  TEXT    PRIMARY KEY,
    title               TEXT,
    secret              TEXT    NOT NULL,
    password_hash       TEXT,
    expires_at          BIGINT,                 -- epoch ms; NULL = never expires
    max_messages        INT     NOT NULL,
    max_participants    INT     NOT NULL,
    rate_limit_ms       BIGINT  NOT NULL,
    max_message_length  INT     NOT NULL,
    moderated           BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id       BIGSERIAL PRIMARY KEY,
    room_id  TEXT      NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    ts       BIGINT    NOT NULL,
    kind     TEXT      NOT NULL,
    payload  TEXT      NOT NULL                -- JSON-encoded event payload
);

CREATE INDEX IF NOT EXISTS events_room_id_idx ON events (room_id, id);

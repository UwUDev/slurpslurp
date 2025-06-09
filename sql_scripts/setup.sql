CREATE TABLE IF NOT EXISTS messages
(
    id                    BIGINT PRIMARY KEY,
    channel_id            BIGINT      NOT NULL,
    author_id             BIGINT      NOT NULL,
    guild_id              BIGINT,
    content               TEXT,
    created_at            TIMESTAMPTZ NOT NULL,
    edited_at             TIMESTAMPTZ,
    message_type          INT         NOT NULL,
    flags                 BIGINT      NOT NULL DEFAULT 0,
    referenced_message_id BIGINT REFERENCES messages (id),
    attachments           JSONB       NOT NULL DEFAULT '[]'::JSONB,
    deleted_at            TIMESTAMPTZ          DEFAULT NULL,
    UNIQUE (id)
);

CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_messages_guild ON messages(guild_id);

CREATE TABLE IF NOT EXISTS users
(
    id           BIGINT PRIMARY KEY,
    username     TEXT NOT NULL,
    global_name  TEXT,
    avatar       TEXT,
    bot          BOOLEAN,
    banner       TEXT,
    accent_color INTEGER,
    flags        INTEGER,
    premium_type INTEGER,
    public_flags INTEGER,
    guilds       BIGINT[] DEFAULT ARRAY []::BIGINT[]
);

CREATE INDEX IF NOT EXISTS idx_users_id ON users(id);

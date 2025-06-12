CREATE TABLE IF NOT EXISTS users
(
    id           BIGINT PRIMARY KEY,
    username     TEXT NOT NULL,
    global_name  TEXT,
    avatar       TEXT,
    bot          BOOLEAN NOT NULL DEFAULT FALSE,
    banner       TEXT,
    accent_color INTEGER,
    flags        INTEGER,
    premium_type INTEGER,
    public_flags INTEGER,
    guilds       BIGINT[] NOT NULL DEFAULT ARRAY []::BIGINT[]
);

CREATE INDEX IF NOT EXISTS idx_users_id ON users (id);

CREATE TABLE IF NOT EXISTS messages
(
    id                    BIGINT PRIMARY KEY,
    channel_id            BIGINT      NOT NULL,
    author_id             BIGINT      NOT NULL REFERENCES users (id),
    guild_id              BIGINT,
    content               TEXT,
    edited_at             TIMESTAMPTZ,
    message_type          INT         NOT NULL,
    flags                 BIGINT      NOT NULL DEFAULT 0,
    referenced_message_id BIGINT REFERENCES messages (id),
    attachments           JSONB       NOT NULL DEFAULT '[]'::JSONB,
    deleted_at            TIMESTAMPTZ          DEFAULT NULL,
    UNIQUE (id)
);

CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages (channel_id);
CREATE INDEX IF NOT EXISTS idx_messages_guild ON messages (guild_id);

CREATE TABLE IF NOT EXISTS guilds
(
    id                       BIGINT PRIMARY KEY,
    name                     TEXT,
    icon                     TEXT,
    region                   TEXT,
    owner_id                 BIGINT,
    member_count             INTEGER,
    features                 TEXT[],
    premium_tier             INTEGER
);

CREATE INDEX IF NOT EXISTS idx_guilds_id ON guilds (id);

CREATE TABLE IF NOT EXISTS roles
(
    id                  BIGINT,
    guild_id            BIGINT NOT NULL REFERENCES guilds (id) ON DELETE CASCADE,
    name                TEXT,
    color               INTEGER,
    hoist               BOOLEAN,
    position            INTEGER,
    permissions         TEXT,
    flags               BIGINT,
    icon                TEXT,
    unicode_emoji       TEXT,
    description         TEXT,
    PRIMARY KEY (id, guild_id)
);

CREATE INDEX IF NOT EXISTS idx_roles_guild ON roles (guild_id);

CREATE TABLE IF NOT EXISTS channels
(
    id                          BIGINT PRIMARY KEY,
    guild_id                    BIGINT REFERENCES guilds (id) ON DELETE CASCADE,
    type                        INTEGER NOT NULL,
    name                        TEXT,
    topic                       TEXT,
    nsfw                        BOOLEAN,
    position                    INTEGER,
    parent_id                   BIGINT,
    flags                       BIGINT,
    permission_overwrites       JSONB DEFAULT '[]'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_channels_guild ON channels (guild_id);
CREATE INDEX IF NOT EXISTS idx_channels_parent ON channels (parent_id);

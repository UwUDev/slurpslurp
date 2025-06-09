use crate::BoxedResult;
use crate::config::Config;
use discord_client_structs::structs::message::{Message, MessageType};
use std::error::Error;
use tokio_postgres::{Client, NoTls};

pub async fn connect_db() -> BoxedResult<Client> {
    let (client, connection) =
        tokio_postgres::connect(Config::get().db_url.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Erreur connexion DB: {}", e);
        }
    });

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS messages (
            id BIGINT PRIMARY KEY,
            channel_id BIGINT NOT NULL,
            author_id BIGINT NOT NULL,
            guild_id BIGINT,
            content TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            edited_at TIMESTAMPTZ,
            message_type INT NOT NULL,
            flags BIGINT NOT NULL DEFAULT 0,
            referenced_message_id BIGINT REFERENCES messages(id),
            attachments JSONB NOT NULL DEFAULT '[]'::JSONB,
            deleted_at TIMESTAMPTZ DEFAULT NULL,
            UNIQUE (id)
        )",
            &[],
        )
        .await?;

    client
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id)",
            &[],
        )
        .await?;

    client
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_guild ON messages(guild_id)",
            &[],
        )
        .await?;

    Ok(client)
}

pub async fn upsert_message(msg: &Message, db: &Client) -> Result<(), Box<dyn Error>> {
    let msg_id: i64 = msg.id as i64;
    let channel_id: i64 = msg.channel_id as i64;
    let author_id: i64 = msg.author.id as i64;
    let flags: i64 = msg.flags as i64;
    let guild_id: Option<i64> = msg.guild_id.map(|id| id as i64);

    let referenced_id: Option<i64> = if let Some(ref_msg) = &msg.referenced_message {
        let id = ref_msg.id as i64;
        let exists: bool = db
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM messages WHERE id = $1)",
                &[&id],
            )
            .await?
            .get(0);
        exists.then_some(id)
    } else {
        None
    };
    let message_type = match msg.r#type {
        MessageType::Default => 0,
        MessageType::RecipientAdd => 1,
        MessageType::RecipientRemove => 2,
        MessageType::Call => 3,
        MessageType::ChannelNameChange => 4,
        MessageType::ChannelIconChange => 5,
        MessageType::ChannelPinnedMessage => 6,
        MessageType::UserJoin => 7,
        MessageType::GuildBoost => 8,
        MessageType::GuildBoostTier1 => 9,
        MessageType::GuildBoostTier2 => 10,
        MessageType::GuildBoostTier3 => 11,
        MessageType::ChannelFollowAdd => 12,
        MessageType::GuildDiscoveryDisqualified => 14,
        MessageType::GuildDiscoveryRequalified => 15,
        MessageType::GuildDiscoveryGracePeriodInitialWarning => 16,
        MessageType::GuildDiscoveryGracePeriodFinalWarning => 17,
        MessageType::ThreadCreated => 18,
        MessageType::Reply => 19,
        MessageType::ChatInputCommand => 20,
        MessageType::ThreadStarterMessage => 21,
        MessageType::GuildInviteReminder => 22,
        MessageType::ContextMenuCommand => 23,
        MessageType::AutoModerationAction => 24,
        MessageType::RoleSubscriptionPurchase => 25,
        MessageType::InteractionPremiumUpsell => 26,
        MessageType::StageStart => 27,
        MessageType::StageEnd => 28,
        MessageType::StageSpeaker => 29,
        MessageType::StageTopic => 31,
        MessageType::GuildApplicationPremiumSubscription => 32,
        MessageType::GuildIncidentAlertModeEnabled => 36,
        MessageType::GuildIncidentAlertModeDisabled => 37,
        MessageType::GuildIncidentReportRaid => 38,
        MessageType::GuildIncidentReportFalseAlarm => 39,
        MessageType::PurchaseNotification => 44,
        MessageType::PollResult => 46,
        MessageType::Unknown(i) => i,
    } as i32;

    db.execute(
        "INSERT INTO messages (
         id, channel_id, author_id, guild_id, content,
         created_at, edited_at, message_type, flags,
         referenced_message_id, attachments
     ) VALUES (
         $1, $2, $3, $4, $5,
         $6, $7, $8, $9,
         $10, $11
     )
     ON CONFLICT (id) DO UPDATE SET
         content   = EXCLUDED.content,
         edited_at = EXCLUDED.edited_at,
         flags     = EXCLUDED.flags,
         attachments = EXCLUDED.attachments",
        &[
            &msg_id,
            &channel_id,
            &author_id,
            &guild_id,
            &msg.content,
            &msg.created_at(),
            &msg.edited_timestamp,
            &message_type,
            &flags,
            &referenced_id,
            &serde_json::to_value(&msg.attachments)?,
        ],
    )
    .await?;

    Ok(())
}

pub async fn delete_message(msg_id: &u64, db: &Client) -> Result<(), Box<dyn Error>> {
    let msg_id = *msg_id as i64;
    db.execute(
        "UPDATE messages SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL",
        &[&msg_id],
    )
    .await?;
    Ok(())
}

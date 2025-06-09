use crate::BoxedResult;
use crate::config::Config;
use discord_client_structs::structs::message::{Message, MessageType};
use discord_client_structs::structs::user::User;
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

pub async fn upsert_user(
    user: &User,
    db: &Client,
    guild_id: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let query = r#"
        INSERT INTO users (id, username, global_name, avatar, bot, banner, accent_color, flags, premium_type, public_flags, guilds)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                CASE WHEN $11::BIGINT IS NOT NULL THEN ARRAY[$11::BIGINT] ELSE ARRAY[]::BIGINT[] END)
        ON CONFLICT (id) DO UPDATE SET
            username = EXCLUDED.username,
            global_name = EXCLUDED.global_name,
            avatar = EXCLUDED.avatar,
            bot = EXCLUDED.bot,
            banner = EXCLUDED.banner,
            accent_color = EXCLUDED.accent_color,
            flags = EXCLUDED.flags,
            premium_type = EXCLUDED.premium_type,
            public_flags = EXCLUDED.public_flags,
            guilds = CASE 
                WHEN $11::BIGINT IS NOT NULL AND NOT ($11::BIGINT = ANY(users.guilds)) THEN 
                    array_append(users.guilds, $11::BIGINT)
                ELSE 
                    users.guilds
            END
    "#;

    db.execute(
        query,
        &[
            &(user.id as i64),
            &user.username,
            &user.global_name,
            &user.avatar,
            &user.bot,
            &user.banner,
            &user.accent_color.map(|v| v as i32),
            &user.flags.map(|v| v as i32),
            &user.premium_type.map(|v| v as i32),
            &user.public_flags.map(|v| v as i32),
            &guild_id.map(|id| id as i64),
        ],
    )
    .await?;

    Ok(())
}

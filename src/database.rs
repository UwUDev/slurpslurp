use crate::BoxedResult;
use crate::config::Config;
use discord_client_structs::structs::channel::Channel;
use discord_client_structs::structs::guild::GatewayGuild;
use discord_client_structs::structs::guild::role::Role;
use discord_client_structs::structs::message::{Message, MessageType};
use discord_client_structs::structs::user::User;
use serde_json;
use std::error::Error;
use tokio_postgres::types::ToSql;
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

pub async fn upsert_message(
    msg: &Message,
    guild_id: Option<u64>,
    db: &Client,
) -> Result<(), Box<dyn Error>> {
    let msg_id: i64 = msg.id as i64;
    let channel_id: i64 = msg.channel_id as i64;
    let author_id: i64 = msg.author.id as i64;
    let flags: i64 = msg.flags as i64;
    let guild_id: Option<i64> = guild_id.map(|id| id as i64);

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
         edited_at, message_type, flags,
         referenced_message_id, attachments
     ) VALUES (
         $1, $2, $3, $4, $5,
         $6, $7, $8, $9,
         $10
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

pub async fn bulk_delete_messages(msg_ids: &[u64], db: &Client) -> Result<(), Box<dyn Error>> {
    if msg_ids.is_empty() {
        return Ok(());
    }

    let mut sql_ids: Vec<i64> = msg_ids.iter().map(|&id| id as i64).collect();
    sql_ids.sort_unstable();

    db.execute(
        "UPDATE messages SET deleted_at = NOW() WHERE id = ANY($1) AND deleted_at IS NULL",
        &[&sql_ids],
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
            &user.bot.unwrap_or(false),
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

pub async fn upsert_guild(
    guild: &GatewayGuild,
    db: &Client,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let guild_id = guild.id as i64;

    if let Some(props) = &guild.properties {
        let name = &props.name;
        let icon = &props.icon;
        let region = &props.region;
        let owner_id = props.owner_id as i64;
        let member_count = guild.member_count.map(|count| count as i32);
        let features = props.features.clone();
        let premium_tier = Some(props.premium_tier as i32);

        db.execute(
            "INSERT INTO guilds (
                id, name, icon, region, owner_id, member_count, features, premium_tier
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                icon = EXCLUDED.icon,
                region = EXCLUDED.region,
                owner_id = EXCLUDED.owner_id,
                member_count = EXCLUDED.member_count,
                features = EXCLUDED.features,
                premium_tier = EXCLUDED.premium_tier",
            &[
                &guild_id,
                &name,
                &icon,
                &region,
                &owner_id,
                &member_count,
                &features,
                &premium_tier,
            ],
        )
        .await?;
    } else {
        // Fallback to using the GatewayGuild fields
        let name = &guild.name;
        let icon = &guild.icon;
        let region = &guild.region;
        let owner_id = 0i64;
        let member_count = guild.member_count.map(|count| count as i32);
        let features: Option<Vec<String>> = guild.features.clone();
        let premium_tier: Option<i32> = None;

        db.execute(
            "INSERT INTO guilds (
                id, name, icon, region, owner_id, member_count, features, premium_tier
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                icon = EXCLUDED.icon,
                region = EXCLUDED.region,
                member_count = EXCLUDED.member_count,
                features = EXCLUDED.features,
                premium_tier = EXCLUDED.premium_tier",
            &[
                &guild_id,
                &name,
                &icon,
                &region,
                &owner_id,
                &member_count,
                &features,
                &premium_tier,
            ],
        )
        .await?;
    }

    Ok(())
}

pub async fn bulk_upsert_roles(
    roles: &[Role],
    guild_id: u64,
    db: &Client,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if roles.is_empty() {
        return Ok(());
    }

    let guild_id_i64 = guild_id as i64;
    let mut role_data = Vec::new();

    for role in roles {
        role_data.push((
            role.id as i64,
            guild_id_i64,
            role.name.clone(),
            role.color as i32,
            role.hoist,
            role.position,
            role.permissions.clone(),
            role.flags.map(|f| f as i64),
            role.icon.clone(),
            role.unicode_emoji.clone(),
            role.description.clone(),
        ));
    }

    let mut placeholders = Vec::new();
    let mut values: Vec<&(dyn ToSql + Sync)> = Vec::new();
    let mut param_index = 1;

    for data in &role_data {
        placeholders.push(format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
            param_index,
            param_index + 1,
            param_index + 2,
            param_index + 3,
            param_index + 4,
            param_index + 5,
            param_index + 6,
            param_index + 7,
            param_index + 8,
            param_index + 9,
            param_index + 10
        ));

        values.extend_from_slice(&[
            &data.0, &data.1, &data.2, &data.3, &data.4, &data.5, &data.6, &data.7, &data.8,
            &data.9, &data.10,
        ]);

        param_index += 11;
    }

    let query = format!(
        "INSERT INTO roles (
            id, guild_id, name, color, hoist, position, permissions,
            flags, icon, unicode_emoji, description
        ) VALUES {}
        ON CONFLICT (id, guild_id) DO UPDATE SET
            name = EXCLUDED.name,
            color = EXCLUDED.color,
            hoist = EXCLUDED.hoist,
            position = EXCLUDED.position,
            permissions = EXCLUDED.permissions,
            flags = EXCLUDED.flags,
            icon = EXCLUDED.icon,
            unicode_emoji = EXCLUDED.unicode_emoji,
            description = EXCLUDED.description",
        placeholders.join(", ")
    );

    db.execute(&query, &values).await?;
    Ok(())
}

pub async fn bulk_upsert_channels(
    channels: &[Channel],
    guild_id: Option<u64>,
    db: &Client,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if channels.is_empty() {
        return Ok(());
    }

    let mut channel_data = Vec::new();

    for channel in channels {
        let permission_overwrites = if let Some(overwrites) = &channel.permission_overwrites {
            Some(serde_json::to_value(overwrites)?)
        } else {
            None
        };

        channel_data.push((
            channel.id as i64,
            guild_id.map(|id| id as i64),
            channel.r#type as i32,
            channel.name.clone(),
            channel.topic.clone(),
            channel.nsfw,
            channel.position.map(|p| p as i32),
            channel.parent_id.map(|id| id as i64),
            channel.flags.map(|f| f as i64),
            permission_overwrites,
        ));
    }

    let mut placeholders = Vec::new();
    let mut values: Vec<&(dyn ToSql + Sync)> = Vec::new();
    let mut param_index = 1;

    for data in &channel_data {
        placeholders.push(format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
            param_index,
            param_index + 1,
            param_index + 2,
            param_index + 3,
            param_index + 4,
            param_index + 5,
            param_index + 6,
            param_index + 7,
            param_index + 8,
            param_index + 9
        ));

        values.extend_from_slice(&[
            &data.0, &data.1, &data.2, &data.3, &data.4, &data.5, &data.6, &data.7, &data.8,
            &data.9,
        ]);

        param_index += 10;
    }

    let query = format!(
        "INSERT INTO channels (
            id, guild_id, type, name, topic, nsfw, position,
            parent_id, flags, permission_overwrites
        ) VALUES {}
        ON CONFLICT (id) DO UPDATE SET
            guild_id = EXCLUDED.guild_id,
            type = EXCLUDED.type,
            name = EXCLUDED.name,
            topic = EXCLUDED.topic,
            nsfw = EXCLUDED.nsfw,
            position = EXCLUDED.position,
            parent_id = EXCLUDED.parent_id,
            flags = EXCLUDED.flags,
            permission_overwrites = EXCLUDED.permission_overwrites",
        placeholders.join(", ")
    );

    db.execute(&query, &values).await?;
    Ok(())
}

pub async fn delete_guild_channels(
    guild_id: u64,
    db: &Client,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let sql_guild_id: i64 = guild_id as i64;
    db.execute("DELETE FROM channels WHERE guild_id = $1", &[&sql_guild_id])
        .await?;

    Ok(())
}

pub async fn delete_guild_roles(
    guild_id: u64,
    db: &Client,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let sql_guild_id: i64 = guild_id as i64;
    db.execute("DELETE FROM roles WHERE guild_id = $1", &[&sql_guild_id])
        .await?;

    Ok(())
}

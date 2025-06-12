use crate::BoxedResult;
use crate::database::*;
use discord_client_gateway::events::structs::channel::ChannelCreateEvent;
use discord_client_gateway::events::structs::guild::role::GuildRoleCreateEvent;
use discord_client_structs::structs::guild::GatewayGuild;
use discord_client_structs::structs::user::{Member, User};
use log::{debug, error};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

pub async fn process_ready_guilds(
    guilds: &Vec<GatewayGuild>,
    ready_members: &Option<Vec<Vec<Member>>>,
    ready_users: &Option<Vec<User>>,
    db: &Client,
) -> BoxedResult<()> {
    if let Some(members_by_guild) = ready_members {
        for (guild_index, members) in members_by_guild.iter().enumerate() {
            if guild_index >= guilds.len() {
                error!(
                    "Guild index out of bounds: {} (guilds.len() = {})",
                    guild_index,
                    guilds.len()
                );
                continue;
            }

            let guild_id = guilds[guild_index].id;

            for member in members {
                if let Some(user) = &member.user {
                    if let Err(e) = upsert_user(user, db, Some(guild_id)).await {
                        error!(
                            "Failed to save user {} in guild {}: {}",
                            user.id, guild_id, e
                        );
                    }
                }
            }
        }
    }

    if let Some(users) = ready_users {
        debug!("Processing {} users from ready event", users.len());
        for user in users {
            if let Err(e) = upsert_user(user, db, None).await {
                error!("Failed to save user {}: {}", user.id, e);
            }
        }
    }

    for guild in guilds {
        if let Err(e) = upsert_guild(guild, db).await {
            error!("Failed to save guild {}: {}", guild.id, e);
            continue;
        }
        debug!(
            "Saved guild: {} ({})",
            guild.name.as_deref().unwrap_or("Unknown"),
            guild.id
        );

        if let Some(roles) = &guild.roles {
            if let Err(e) = delete_guild_roles(guild.id, db).await {
                error!("Failed to clear old roles for guild {}: {}", guild.id, e);
            }

            for role in roles {
                if let Err(e) = bulk_upsert_roles(&[role.clone()], guild.id, db).await {
                    error!(
                        "Failed to save role {} in guild {}: {}",
                        role.id, guild.id, e
                    );
                }
            }
            debug!("Saved {} roles for guild {}", roles.len(), guild.id);
        }

        if let Some(channels) = &guild.channels {
            if let Err(e) = delete_guild_channels(guild.id, db).await {
                error!("Failed to clear old channels for guild {}: {}", guild.id, e);
            }

            for channel in channels {
                if let Err(e) = bulk_upsert_channels(&[channel.clone()], Some(guild.id), db).await {
                    error!(
                        "Failed to save channel {} in guild {}: {}",
                        channel.id, guild.id, e
                    );
                }
            }
            debug!("Saved {} channels for guild {}", channels.len(), guild.id);
        }

        if let Some(threads) = &guild.threads {
            for thread in threads {
                if let Err(e) = bulk_upsert_channels(&[thread.clone()], Some(guild.id), db).await {
                    error!(
                        "Failed to save thread {} in guild {}: {}",
                        thread.id, guild.id, e
                    );
                }
            }
            if !threads.is_empty() {
                debug!("Saved {} threads for guild {}", threads.len(), guild.id);
            }
        }
    }

    Ok(())
}

pub async fn process_channel_create(
    channel_create: &ChannelCreateEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if let Some(db_client) = db_client {
        let db_client = db_client.lock().await;
        if let Err(e) =
            bulk_upsert_channels(&[channel_create.channel.clone()], None, &db_client).await
        {
            error!(
                "Failed to save channel {}: {}",
                channel_create.channel.id, e
            );
        } else {
            debug!("Channel {} created and saved", channel_create.channel.id);
        }
    }

    Ok(())
}

pub async fn process_channel_update(
    channel_update: &discord_client_gateway::events::structs::channel::ChannelUpdateEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if let Some(db_client) = db_client {
        let db_client = db_client.lock().await;
        if let Err(e) =
            bulk_upsert_channels(&[channel_update.channel.clone()], None, &db_client).await
        {
            error!(
                "Failed to update channel {}: {}",
                channel_update.channel.id, e
            );
        } else {
            debug!("Channel {} updated successfully", channel_update.channel.id);
        }
    }

    Ok(())
}

pub async fn process_channel_delete(
    channel_delete: &discord_client_gateway::events::structs::channel::ChannelDeleteEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if let Some(db_client) = db_client {
        let db_client = db_client.lock().await;
        if let Err(e) = delete_channel(channel_delete.channel.id, &db_client).await {
            error!(
                "Failed to delete channel {}: {}",
                channel_delete.channel.id, e
            );
        } else {
            debug!("Channel {} deleted successfully", channel_delete.channel.id);
        }
    }
    Ok(())
}

pub async fn process_role_create(
    role_create: &GuildRoleCreateEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if let Some(db_client) = db_client {
        let db_client = db_client.lock().await;
        if let Err(e) = bulk_upsert_roles(
            &[role_create.role.clone()],
            role_create.guild_id,
            &db_client,
        )
        .await
        {
            error!(
                "Failed to save role {} in guild {}: {}",
                role_create.role.id, role_create.guild_id, e
            );
        } else {
            debug!(
                "Role {} created and saved in guild {}",
                role_create.role.id, role_create.guild_id
            );
        }
    }

    Ok(())
}

pub async fn process_role_update(
    role_update: &discord_client_gateway::events::structs::guild::role::GuildRoleUpdateEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if let Some(db_client) = db_client {
        let db_client = db_client.lock().await;
        if let Err(e) = bulk_upsert_roles(
            &[role_update.role.clone()],
            role_update.guild_id,
            &db_client,
        )
        .await
        {
            error!(
                "Failed to update role {} in guild {}: {}",
                role_update.role.id, role_update.guild_id, e
            );
        } else {
            debug!(
                "Role {} updated successfully in guild {}",
                role_update.role.id, role_update.guild_id
            );
        }
    }

    Ok(())
}

pub async fn process_role_delete(
    role_delete: &discord_client_gateway::events::structs::guild::role::GuildRoleDeleteEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if let Some(db_client) = db_client {
        let db_client = db_client.lock().await;
        if let Err(e) = delete_role(role_delete.role_id, &db_client).await {
            error!(
                "Failed to delete role {} in guild {}: {}",
                role_delete.role_id, role_delete.guild_id, e
            );
        } else {
            debug!(
                "Role {} deleted successfully in guild {}",
                role_delete.role_id, role_delete.guild_id
            );
        }
    }

    Ok(())
}

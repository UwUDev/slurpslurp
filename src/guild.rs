use crate::BoxedResult;
use crate::database::*;
use discord_client_structs::structs::guild::GatewayGuild;
use discord_client_structs::structs::user::{Member, User};
use log::{debug, error};

pub async fn process_ready_guilds(
    guilds: &Vec<GatewayGuild>,
    ready_members: &Option<Vec<Vec<Member>>>,
    ready_users: &Option<Vec<User>>,
    db: &tokio_postgres::Client,
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
            debug!(
                "Processing {} members for guild {}",
                members.len(),
                guild_id
            );

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

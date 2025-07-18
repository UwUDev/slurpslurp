use crate::BoxedResult;
use crate::database::bulk_upsert_users;
use discord_client_gateway::events::structs::guild::GuildMemberUpdateEvent;
use discord_client_gateway::events::structs::requested::GuildMembersChunkEvent;
use log::error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

pub async fn process_guild_members_chunk(
    members_chunk: &GuildMembersChunkEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if let Some(client) = db_client {
        let client = client.lock().await;

        let users = members_chunk
            .members
            .clone()
            .into_iter()
            .filter_map(|member| member.user)
            .collect::<Vec<_>>();

        bulk_upsert_users(users.as_slice(), &client).await?;
    }

    Ok(())
}

pub async fn process_guild_member_update(
    event: &GuildMemberUpdateEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if let Some(client) = db_client {
        let client = client.lock().await;
        let user = &event.user;
        let guild_id = event.guild_id;

        if let Err(e) = crate::database::upsert_user(user, &client, Some(guild_id)).await {
            error!(
                "Failed to upsert user {} in guild {}: {}",
                user.id, guild_id, e
            );
        }
    }

    Ok(())
}

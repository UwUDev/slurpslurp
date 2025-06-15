use crate::BoxedResult;
use crate::database::bulk_upsert_users;
use discord_client_gateway::events::structs::ready::ReadySupplementalEvent;
use discord_client_structs::structs::user::User;
use tokio_postgres::Client;

pub async fn process_ready_supplemental(
    ready_supplemental: &ReadySupplementalEvent,
    client: &Client,
) -> BoxedResult<()> {
    let users: Vec<User> = {
        let lazy_users: Vec<User> = ready_supplemental
            .lazy_private_channels
            .iter()
            .filter_map(|channel| channel.recipients.clone())
            .flatten()
            .collect();

        let mut users: Vec<User> = ready_supplemental
            .clone()
            .merged_members
            .into_iter()
            .flatten()
            .filter_map(|member| member.user)
            .collect();

        users.extend(lazy_users);
        users
    };

    bulk_upsert_users(users.as_slice(), client).await
}

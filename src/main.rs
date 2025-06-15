mod config;
mod database;
mod downloader;
mod guild;
mod message;
mod user;

use crate::config::Config;
use crate::database::{bulk_upsert_users, connect_db};
use crate::guild::*;
use crate::message::*;
use crate::user::process_guild_members_chunk;
use discord_client_gateway::events::Event;
use discord_client_gateway::events::structs::ready::ReadySupplementalEvent;
use discord_client_gateway::gateway::GatewayClient;
use discord_client_rest::rest::RestClient;
use discord_client_structs::structs::user::User;
use log::{debug, error, info, warn};
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, atomic};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_postgres::Client;

type BoxedError = Box<dyn Error + Send + Sync>;
type BoxedResult<T> = Result<T, BoxedError>;

#[tokio::main]
async fn main() -> BoxedResult<()> {
    pretty_env_logger::formatted_builder()
        .filter(None, log::LevelFilter::Off)
        .filter_module("slurpslurp", log::LevelFilter::Debug)
        .init();

    if let Err(e) = Config::init() {
        error!("Error initializing config: {}", e);
        std::process::exit(1);
    }

    if !std::path::Path::new("downloads").exists() {
        std::fs::create_dir("downloads")?;
        debug!("Created downloads directory");
    }

    let db_client = if Config::get().use_db {
        Some(Arc::new(Mutex::new(connect_db().await.map_err(|e| {
            format!("Error connecting to database: {}", e)
        })?)))
    } else {
        None
    };

    let setup_script = include_str!("../sql_scripts/setup.sql");
    if let Some(ref db) = db_client {
        let client = db.lock().await;
        client
            .batch_execute(setup_script)
            .await
            .map_err(|e| format!("Error executing setup script: {}", e))?;

        debug!("Database setup script executed successfully");
    }

    let tokens_content = std::fs::read_to_string("tokens.txt")
        .map_err(|e| format!("Error reading tokens.txt: {}", e))?;

    let tokens: Vec<String> = tokens_content
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();

    if tokens.is_empty() {
        error!("No tokens found in tokens.txt");
        return Err("No valid tokens".into());
    }

    info!("Starting {} accounts", tokens.len());

    let mut handles = Vec::new();

    let rest_client = RestClient::connect(tokens.get(0).unwrap().clone(), Some(9), None)
        .await
        .map_err(|e| format!("Error connecting to Discord REST API: {}", e))?;

    let build_number = rest_client.build_number;
    debug!("Retrieved latest client build number: {}", build_number);

    for (index, token) in tokens.into_iter().enumerate() {
        let db_client_clone = if let Some(ref db) = db_client {
            Some(Arc::clone(db))
        } else {
            None
        };

        let handle = tokio::spawn(async move {
            if let Err(e) = handle_account(token, index, db_client_clone, build_number).await {
                error!("Error with account {}: {}", index, e);
            }
        });

        handles.push(handle);

        tokio::time::sleep(Duration::from_millis(600)).await;
    }

    for handle in handles {
        if let Err(e) = handle.await {
            error!("Error in task: {}", e);
        }
    }

    Ok(())
}

// delay for asking 1000 most recent guild joins (10 minutes)
const REQUEST_DELAY: Duration = Duration::from_secs(600);

async fn handle_account(
    token: String,
    account_index: usize,
    db_client: Option<Arc<Mutex<Client>>>,
    build_number: u32,
) -> BoxedResult<()> {
    loop {
        info!("Connecting account {} ...", account_index);

        let mut gateway_client =
            GatewayClient::connect(token.clone(), true, 53607934, build_number)
                .await
                .map_err(|e| format!("Gateway error for account {}: {}", account_index, e))?;

        info!("Account {} connected successfully", account_index);

        let mut last_request = Instant::now();
        let ids: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let id_index: AtomicUsize = AtomicUsize::new(0);

        loop {
            let event = gateway_client.next_event().await;
            match event {
                Ok(Event::Ready(ready)) => {
                    let guilds = ready.guilds;

                    if let Some(ref db) = db_client {
                        let client = db.lock().await;
                        process_ready_guilds(&guilds, &ready.merged_members, &ready.users, &client)
                            .await?;
                    }

                    ids.lock().await.clear();

                    for guild in guilds {
                        let guild_id = guild.id;
                        ids.lock().await.push(guild_id);
                    }

                    let count = ids.lock().await.len();
                    gateway_client
                        .bulk_guild_subscribe(ids.lock().await.clone())
                        .await
                        .map_err(|e| format!("Error subscribing to guilds: {}", e))?;
                    debug!("Account {} : Subscribed to {} guilds", account_index, count);

                    if count > id_index.load(atomic::Ordering::Relaxed) {
                        id_index.store(0, atomic::Ordering::Relaxed);
                    }
                }
                Ok(Event::ReadySupplemental(ready_supplemental)) => {
                    if let Some(ref db) = db_client {
                        let client = db.lock().await;
                        process_ready_supplemental(&ready_supplemental, &client).await?;
                    }
                }
                Ok(Event::MessageCreate(msg_create)) => {
                    if let Err(e) = process_message_create(&msg_create, &db_client).await {
                        warn!(
                            "Account {} : Error processing message: {}",
                            account_index, e
                        );
                    }
                }
                Ok(Event::MessageUpdate(msg_update)) => {
                    if let Err(e) = process_message_update(&msg_update, &db_client).await {
                        error!("Account {} : Error updating message: {}", account_index, e);
                    }
                }
                Ok(Event::MessageDelete(msg_delete)) => {
                    if let Err(e) = process_message_delete(&msg_delete, &db_client).await {
                        error!("Account {} : Error deleting message: {}", account_index, e);
                    }
                }
                Ok(Event::MessageDeleteBulk(msg_delete_bulk)) => {
                    if let Err(e) = process_message_delete_bulk(&msg_delete_bulk, &db_client).await
                    {
                        error!(
                            "Account {} : Error deleting bulk messages: {}",
                            account_index, e
                        );
                    }
                }
                Ok(Event::ChannelCreate(channel_create)) => {
                    if let Err(e) = process_channel_create(&channel_create, &db_client).await {
                        error!("Account {} : Error creating channel: {}", account_index, e);
                    }
                }
                Ok(Event::ChannelUpdate(channel_update)) => {
                    if let Err(e) = process_channel_update(&channel_update, &db_client).await {
                        error!("Account {} : Error updating channel: {}", account_index, e);
                    }
                }
                Ok(Event::ChannelDelete(channel_delete)) => {
                    if let Err(e) = process_channel_delete(&channel_delete, &db_client).await {
                        error!("Account {} : Error deleting channel: {}", account_index, e);
                    }
                }
                Ok(Event::GuildRoleCreate(role_create)) => {
                    if let Err(e) = process_role_create(&role_create, &db_client).await {
                        error!("Account {} : Error creating role: {}", account_index, e);
                    }
                }
                Ok(Event::GuildRoleUpdate(role_update)) => {
                    if let Err(e) = process_role_update(&role_update, &db_client).await {
                        error!("Account {} : Error updating role: {}", account_index, e);
                    }
                }
                Ok(Event::GuildRoleDelete(role_delete)) => {
                    if let Err(e) = process_role_delete(&role_delete, &db_client).await {
                        error!("Account {} : Error deleting role: {}", account_index, e);
                    }
                }
                Ok(Event::GuildMembersChunk(members_chunk)) => {
                    if let Err(e) = process_guild_members_chunk(&members_chunk, &db_client).await {
                        error!(
                            "Account {} : Error processing guild members chunk: {}",
                            account_index, e
                        );
                    }
                }
                Err(e) => {
                    error!("Event error account {}: {}", account_index, e);
                    // if client error (Connect) break the loop to reconnect
                    if e.to_string().contains("client error (Connect)") {
                        info!("Reconnecting account {} in 5 seconds...", account_index);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        break;
                    }
                }
                _ => (),
            }

            if db_client.is_some() {
                if Instant::now().duration_since(last_request) >= REQUEST_DELAY {
                    let index = id_index.load(atomic::Ordering::Relaxed);
                    if let Some(guild_id) = ids.lock().await.get(index) {
                        if let Err(e) = gateway_client
                            .search_recent_members(*guild_id, "", None, None)
                            .await
                        {
                            error!(
                                "Account {} : Error requesting guild members: {}",
                                account_index, e
                            );
                        }
                    }

                    if index + 1 >= ids.lock().await.len() {
                        id_index.store(0, atomic::Ordering::Relaxed);
                    } else {
                        id_index.fetch_add(1, atomic::Ordering::Relaxed);
                    }
                    last_request = Instant::now();
                }
            }
        }
    }
}

async fn process_ready_supplemental(
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

mod config;
mod database;
mod downloader;
mod message;

use crate::config::Config;
use crate::database::connect_db;
use crate::message::*;
use discord_client_gateway::events::Event;
use discord_client_gateway::gateway::GatewayClient;
use discord_client_rest::rest::RestClient;
use log::{debug, error, info, warn};
use std::error::Error;
use std::sync::Arc;
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

        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
    }

    for handle in handles {
        if let Err(e) = handle.await {
            error!("Error in task: {}", e);
        }
    }

    Ok(())
}

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

        loop {
            let event = gateway_client.next_event().await;
            match event {
                Ok(Event::Ready(ready)) => {
                    let mut ids: Vec<u64> = Vec::new();
                    let guilds = ready.guilds;
                    for guild in guilds {
                        let guild_id = guild.id;
                        ids.push(guild_id);
                    }

                    let count = ids.len();
                    gateway_client.bulk_guild_subscribe(ids).await?;
                    debug!("Account {} : Subscribed to {} guilds", account_index, count);
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
                Err(e) => {
                    error!("Event error account {}: {}", account_index, e);
                    // if client error (Connect) break the loop to reconnect
                    if e.to_string().contains("client error (Connect)") {
                        info!("Reconnecting account {} in 5 seconds...", account_index);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        break;
                    }
                }
                _ => (),
            }
        }
    }
}

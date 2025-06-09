use crate::config::Config;
use crate::database::{delete_message, upsert_message};
use crate::downloader;
use discord_client_gateway::events::structs::message::{
    MessageCreateEvent, MessageDeleteEvent, MessageUpdateEvent,
};
use discord_client_structs::structs::message::Message;
use discord_client_structs::structs::user::User;
use log::{error, info};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

async fn process_message_common(
    msg: &Message,
    user: &User,
    db_client: &Option<Arc<Mutex<Client>>>,
    log_content: bool,
) -> Result<(), Box<dyn Error>> {
    if Config::get().skip_bot_messages && user.bot.unwrap_or(false) {
        return Ok(());
    }

    if log_content {
        if let Some(content) = &msg.content {
            info!("{}: {}", user.username, content);
        }
    }

    if let Some(db_client) = db_client {
        let db_client = db_client.lock().await;

        if let Err(e) = upsert_message(msg, &db_client).await {
            error!("Failed to save message: {}", e);
        }
    }

    // spawn a task to download attachments
    if Config::get().download_files {
        if !msg.attachments.is_empty() {
            let attachments = msg.attachments.clone();

            tokio::spawn(async move {
                if let Err(e) = downloader::download_attachment(attachments).await {
                    error!("Failed to download attachments: {}", e);
                }
            });
        }

        if !msg.embeds.is_empty() {
            let embeds = msg.embeds.clone();
            let message_id = msg.id;

            tokio::spawn(async move {
                if let Err(e) = downloader::download_embeds(embeds, message_id).await {
                    error!("Failed to download embeds: {}", e);
                }
            });
        }
    }

    Ok(())
}

pub async fn process_message_create(
    msg_create: &MessageCreateEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> Result<(), Box<dyn Error>> {
    process_message_common(
        &msg_create.message,
        &msg_create.message.author,
        db_client,
        true,
    )
    .await
}

pub async fn process_message_update(
    msg_update: &MessageUpdateEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> Result<(), Box<dyn Error>> {
    process_message_common(
        &msg_update.message,
        &msg_update.message.author,
        db_client,
        false,
    )
    .await
}

pub async fn process_message_delete(
    msg_delete: &MessageDeleteEvent,
    db_client: &Option<Arc<Mutex<Client>>>,
) -> Result<(), Box<dyn Error>> {
    if let Some(db_client) = db_client {
        let db_client = db_client.lock().await;
        let msg_id = &msg_delete.id;

        if let Err(e) = delete_message(msg_id, &db_client).await {
            error!("Failed to delete message: {}", e);
        }
    }

    Ok(())
}

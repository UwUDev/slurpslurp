use crate::BoxedResult;
use crate::config::Config;
use crate::event_processor::message::process_message_common;
use clap::ValueEnum;
use discord_client_rest::rest::RestClient;
use discord_client_structs::structs::message::Message;
use discord_client_structs::structs::message::query::{
    MessageQueryBuilder, MessageSearchQueryBuilder,
};
use log::{error, info};
use progress_bar::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

pub struct Scraper {
    pub bots: Vec<RestClient>,
    id: u64,
    scrape_type: ScrapeType,
    db_client: Option<Arc<Mutex<Client>>>,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum ScrapeType {
    Channel,
    Guild,
}

impl Scraper {
    pub async fn new(
        tokens: Vec<String>,
        id: u64,
        scrape_type: ScrapeType,
        db_client: Option<Arc<Mutex<Client>>>,
    ) -> Scraper {
        let mut bots = Vec::new();
        for token in tokens {
            match RestClient::connect(token.clone(), Some(9), None).await {
                Ok(client) => bots.push(client),
                Err(e) => eprintln!("Failed to connect with token: {}. Error: {}", token, e),
            }
        }
        Scraper {
            bots,
            id,
            scrape_type,
            db_client,
        }
    }

    pub async fn start(&self) -> BoxedResult<()> {
        if self.bots.is_empty() {
            return Err("No valid bots connected for scraping".into());
        }

        let mut bot_index = 0;
        let mut last_message_id: Option<u64> = None;
        let mut progress_bar_initialized = false;
        let mut progress = 0;
        let mut last_id: u64 = (chrono::Utc::now().timestamp_millis() << 22) as u64;
        loop {
            if bot_index >= self.bots.len() {
                bot_index = 0;
            }

            let bot = &self.bots[bot_index];
            match self.scrape_type {
                ScrapeType::Channel => {
                    let message_rest = bot.message(self.id);
                    let mut builder = MessageQueryBuilder::default();
                    builder.limit(100);

                    if let Some(last_id) = last_message_id {
                        builder.before(last_id);
                    }

                    let query = builder.build()?;

                    // Rate limit handled by the crate
                    let messages = message_rest.get_channel_messages(None, query).await;
                    if let Err(e) = messages {
                        error!("Error fetching messages: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    let messages = messages?;

                    if messages.is_empty() {
                        info!(
                            "Bot {}: No more messages to scrape in channel {}",
                            bot_index, self.id
                        );
                        break;
                    }

                    for message in &messages {
                        process_message_common(
                            message,
                            &message.author,
                            Some(self.id),
                            &self.db_client,
                            true,
                        )
                        .await
                        .unwrap();
                    }

                    let lowest = messages
                        .iter()
                        .min_by_key(|m| m.id)
                        .map(|m| m.id)
                        .unwrap_or_default();

                    last_message_id = Some(lowest);
                }
                ScrapeType::Guild => {
                    let guild_rest = bot.guild(Some(self.id));
                    let query = MessageSearchQueryBuilder::default()
                        .max_id(last_id)
                        .include_nsfw(true)
                        .build()?;

                    let search_result = guild_rest.search_guild_messages(query).await?;
                    if !progress_bar_initialized {
                        init_progress_bar(search_result.total_results as usize);
                        set_progress_bar_action("Scraping", Color::Blue, Style::Bold);
                        progress_bar_initialized = true;
                    }

                    let mut messages: Vec<Message> =
                        search_result.messages.into_iter().flatten().collect();

                    let count = messages.len();

                    last_id = messages
                        .iter()
                        .min_by_key(|m| m.id)
                        .map(|m| m.id)
                        .unwrap_or_default();

                    if Config::get().skip_bot_messages {
                        messages = messages
                            .into_iter()
                            .filter(|msg| !msg.author.bot.unwrap_or(false))
                            .collect();
                    }

                    progress += count;

                    set_progress_bar_progress(progress);

                    if count == 0 {
                        print_progress_bar_info(
                            "Finished",
                            "No more messages to scrape in guild",
                            Color::Green,
                            Style::Bold,
                        );
                        break;
                    }

                    for message in messages {
                        process_message_common(
                            &message,
                            &message.author,
                            Some(self.id),
                            &self.db_client,
                            false,
                        )
                        .await
                        .unwrap();
                    }
                }
            }

            bot_index += 1;
        }

        Ok(())
    }
}

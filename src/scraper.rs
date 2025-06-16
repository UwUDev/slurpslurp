use crate::BoxedResult;
use crate::event_processor::message::process_message_common;
use clap::ValueEnum;
use discord_client_rest::rest::RestClient;
use discord_client_structs::structs::message::query::MessageQueryBuilder;
use log::info;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

pub struct Scraper {
    pub bots: Vec<RestClient>,
    id: u64,
    scrape_type: ScrapeType,
    db_client: Option<Arc<Mutex<Client>>>,
}

#[derive(ValueEnum, Clone, Debug)]
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
        loop {
            if bot_index >= self.bots.len() {
                bot_index = 0; // Reset index to loop through bots
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
                    let messages = message_rest.get_channel_messages(None, query).await?;

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
                    todo!("Guild scraping not implemented yet");
                }
            }

            bot_index += 1;
        }

        Ok(())
    }
}

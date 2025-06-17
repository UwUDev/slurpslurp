use crate::BoxedResult;
use crate::config::Config;
use crate::event_processor::message::process_message_common;
use clap::ValueEnum;
use discord_client_rest::rest::RestClient;
use discord_client_structs::structs::message::Message;
use discord_client_structs::structs::message::query::{
    MessageQuery, MessageQueryBuilder, MessageSearchQueryBuilder, MessageSearchResult,
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
        let mut scrape_state = ScrapeState::new();

        loop {
            if bot_index >= self.bots.len() {
                bot_index = 0;
            }

            let bot = &self.bots[bot_index];

            let should_continue = match self.scrape_type {
                ScrapeType::Channel => {
                    self.scrape_channel(bot, bot_index, &mut scrape_state)
                        .await?
                }
                ScrapeType::Guild => self.scrape_guild(bot, &mut scrape_state).await?,
            };

            if !should_continue {
                break;
            }

            bot_index += 1;
        }

        Ok(())
    }

    async fn scrape_channel(
        &self,
        bot: &RestClient,
        bot_index: usize,
        state: &mut ScrapeState,
    ) -> BoxedResult<bool> {
        let message_rest = bot.message(self.id);
        let query = self.build_channel_query(state.last_message_id)?;

        let messages = match message_rest.get_channel_messages(None, query).await {
            Ok(messages) => messages,
            Err(e) => {
                error!("Error fetching messages: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                return Ok(true); // Continue with the next bot
            }
        };

        if messages.is_empty() {
            info!(
                "Bot {}: No more messages to scrape in channel {}",
                bot_index, self.id
            );
            return Ok(false); // Scraping done for this channel
        }

        self.process_messages(&messages, true).await?;

        state.last_message_id = Some(
            messages
                .iter()
                .min_by_key(|m| m.id)
                .map(|m| m.id)
                .unwrap_or_default(),
        );

        Ok(true)
    }

    async fn scrape_guild(&self, bot: &RestClient, state: &mut ScrapeState) -> BoxedResult<bool> {
        let guild_rest = bot.guild(Some(self.id));
        let query = MessageSearchQueryBuilder::default()
            .max_id(state.last_id)
            .include_nsfw(true)
            .build()?;

        let search_result = guild_rest.search_guild_messages(query).await?;

        self.initialize_progress_bar_if_needed(&search_result, &mut state.progress_bar_initialized);

        let mut messages: Vec<Message> = search_result.messages.into_iter().flatten().collect();
        let count = messages.len();

        if count == 0 {
            print_progress_bar_info(
                "Finished",
                "No more messages to scrape in guild",
                Color::Green,
                Style::Bold,
            );
            return Ok(false); // Scraping done for this guild
        }

        state.last_id = messages
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

        state.progress += count;
        set_progress_bar_progress(state.progress);

        self.process_messages(&messages, false).await?;

        Ok(true)
    }

    fn build_channel_query(&self, last_message_id: Option<u64>) -> BoxedResult<MessageQuery> {
        let mut builder = MessageQueryBuilder::default();
        builder.limit(100);

        if let Some(last_id) = last_message_id {
            builder.before(last_id);
        }

        Ok(builder.build()?)
    }

    async fn process_messages(&self, messages: &[Message], is_channel: bool) -> BoxedResult<()> {
        for message in messages {
            process_message_common(
                message,
                &message.author,
                Some(self.id),
                &self.db_client,
                is_channel,
            )
            .await
            .unwrap();
        }
        Ok(())
    }

    fn initialize_progress_bar_if_needed(
        &self,
        search_result: &MessageSearchResult,
        progress_bar_initialized: &mut bool,
    ) {
        if !*progress_bar_initialized {
            init_progress_bar(search_result.total_results as usize);
            set_progress_bar_action("Scraping", Color::Blue, Style::Bold);
            *progress_bar_initialized = true;
        }
    }
}

struct ScrapeState {
    last_message_id: Option<u64>,
    progress_bar_initialized: bool,
    progress: usize,
    last_id: u64,
}

impl ScrapeState {
    fn new() -> Self {
        Self {
            last_message_id: None,
            progress_bar_initialized: false,
            progress: 0,
            last_id: (chrono::Utc::now().timestamp_millis() << 22) as u64,
        }
    }
}

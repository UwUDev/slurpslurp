mod cli;
mod config;
mod database;
mod downloader;
mod event_processor;
mod handler;
mod scraper;

use crate::cli::{Cli, Mode};
use crate::config::Config;
use crate::database::connect_db;
use crate::handler::handle_account;
use crate::scraper::*;
use clap::Parser;
use discord_client_rest::rest::RestClient;
use log::{debug, error, info, warn};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
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

    let cli = Cli::parse();
    if cli.help {
        todo!("Implement clap-help functionality");
    }

    let mode = cli.mode.unwrap_or(Mode::Sniff);

    if let Err(e) = Config::init() {
        error!("Error initializing config: {}", e);
        std::process::exit(1);
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

    match mode {
        Mode::Sniff => start_sniff(db_client).await?,
        Mode::Scrape {
            target_type,
            id,
            tokens,
        } => {
            start_scrape(target_type, id, tokens, db_client).await?;
        }
    }

    Ok(())
}

async fn start_sniff(db_client: Option<Arc<Mutex<Client>>>) -> BoxedResult<()> {
    info!("Starting sniff mode...");

    if !std::path::Path::new("downloads").exists() {
        std::fs::create_dir("downloads")?;
        debug!("Created downloads directory");
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

async fn start_scrape(
    target_type: ScrapeType,
    id: u64,
    tokens: Vec<String>,
    db_client: Option<Arc<Mutex<Client>>>,
) -> BoxedResult<()> {
    if tokens.is_empty() {
        error!("No tokens provided for scraping");
        return Err("No valid tokens".into());
    }

    info!("Starting scrape mode...");
    if target_type == ScrapeType::Guild && tokens.len() < 3 {
        warn!(
            "Guild scraping is way slower than channel scraping with a low amount of tokens. I'd recommend to run multiple channel scrapers instead."
        );
    }

    let scraper = Scraper::new(tokens, id, target_type, db_client).await;

    if scraper.bots.is_empty() {
        error!("No valid bots connected for scraping");
        return Err("No valid bots".into());
    }

    info!("Starting scraping with {} bots", scraper.bots.len());

    if let Err(e) = scraper.start().await {
        error!("Error during scraping: {}", e);
        return Err(e);
    }

    Ok(())
}

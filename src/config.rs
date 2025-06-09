use log::{error, info};
use serde::Deserialize;
use std::error::Error;
use std::process::exit;
use std::sync::OnceLock;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub skip_bot_messages: bool,
    pub download_files: bool,
    pub use_db: bool,
    pub db_url: String,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

impl Config {
    pub fn init() -> Result<(), Box<dyn Error>> {
        if !std::path::Path::new("config.toml").exists() {
            if std::path::Path::new("config_example.toml").exists() {
                error!(
                    "Please rename 'config_example.toml' to 'config.toml' and fill in the required fields."
                );
            } else {
                error!("Configuration file 'config.toml' is missing.");
            }
            exit(1);
        }

        let config_content = std::fs::read_to_string("config.toml")?;
        let config: Config = toml::from_str(&config_content)?;

        CONFIG
            .set(config)
            .map_err(|_| "Configuration already initialized")?;

        info!("Config initialized.");
        Ok(())
    }

    pub fn get() -> &'static Config {
        CONFIG.get().expect("Configuration not initialized")
    }
}

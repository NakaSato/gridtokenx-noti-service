//! Service configuration loaded from environment / config files.

use serde::Deserialize;

use crate::error::{NotiError, Result};

fn default_log_level() -> String {
    "info".to_string()
}

fn default_port() -> u16 {
    8080
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,
    pub database_url: String,
    pub kafka_brokers: String,
    pub rabbitmq_url: String,
    pub redis_url: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,

    // External Provider Keys (optional)
    pub twilio_account_sid: Option<String>,
    pub twilio_auth_token: Option<String>,
    pub fcm_project_id: Option<String>,
    
    // SMTP Configuration
    pub smtp_host: Option<String>,
    pub smtp_port: Option<u16>,
    pub smtp_user: Option<String>,
    pub smtp_pass: Option<String>,
    pub smtp_from: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let run_mode = std::env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::File::with_name(&format!("config/{run_mode}")).required(false))
            // Support standard names like DATABASE_URL, REDIS_URL, PORT
            .add_source(config::Environment::default().separator("__"))
            // Support prefixed overrides like APP__PORT
            .add_source(config::Environment::with_prefix("APP").separator("__"))
            .build()
            .map_err(|e| NotiError::Internal(format!("config build error: {e}")))?;

        s.try_deserialize()
            .map_err(|e| NotiError::Internal(format!("config deserialize error: {e}")))
    }
}

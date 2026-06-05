//! Service configuration loaded from environment / config files.

use serde::Deserialize;

use crate::error::{NotiError, Result};

fn default_log_level() -> String {
    "info".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_db_max_connections() -> u32 {
    20
}

fn default_db_min_connections() -> u32 {
    5
}

fn default_db_acquire_timeout() -> u64 {
    30
}

fn default_db_idle_timeout() -> u64 {
    600
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

    // Database Pooling
    #[serde(default = "default_db_max_connections")]
    pub database_max_connections: u32,
    #[serde(default = "default_db_min_connections")]
    pub database_min_connections: u32,
    #[serde(default = "default_db_acquire_timeout")]
    pub database_acquire_timeout_secs: u64,
    #[serde(default = "default_db_idle_timeout")]
    pub database_idle_timeout_secs: u64,

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
    /// TLS mode: "starttls" (default), "tls" (implicit, port 465), "none" (no TLS, e.g. Mailpit)
    pub smtp_tls_mode: Option<String>,

    // JWT
    pub jwt_secret: String,

    // Frontend
    /// Base URL for the frontend (used to construct email callback links).
    pub frontend_url: Option<String>,

    // GRPC
    pub grpc_port: Option<u16>,

    // Kafka Topics
    #[serde(default = "default_kafka_user_events")]
    pub kafka_topic_user_events: String,
    #[serde(default = "default_kafka_audit_events")]
    pub kafka_topic_audit_events: String,
}

fn default_kafka_user_events() -> String {
    "iam.user.events".to_string()
}

fn default_kafka_audit_events() -> String {
    "iam.audit.events".to_string()
}

impl Config {
    /// # Errors
    ///
    /// Returns `NotiError::Internal` if configuration sources cannot be
    /// read or deserialized.
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

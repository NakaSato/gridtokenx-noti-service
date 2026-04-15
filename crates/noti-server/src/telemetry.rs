//! Telemetry initialization (structured logging + metrics).

use tracing_subscriber::{fmt, EnvFilter};

/// Initialize the tracing subscriber with JSON output and env-based filtering.
pub fn init_telemetry(service_name: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .json()
        .flatten_event(true)
        .init();

    tracing::info!(service = service_name, "📡 Telemetry initialized");
}

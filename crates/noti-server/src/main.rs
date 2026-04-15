use noti_core::config::Config;
use noti_server::{startup, telemetry};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    // 1. Load environment variables
    dotenvy::dotenv().ok();

    // 2. Initialize telemetry (tracing + metrics)
    telemetry::init_telemetry("gridtokenx-noti");

    // 3. Load configuration
    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            std::process::exit(1);
        }
    };

    // 4. Lifecycle coordination
    let shutdown_token = CancellationToken::new();
    let service_token = shutdown_token.clone();

    tokio::spawn(async move {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            () = ctrl_c => {
                info!("🛑 SIGINT received, triggering shutdown...");
            },
            () = terminate => {
                info!("🛑 SIGTERM received, triggering shutdown...");
            },
        }

        shutdown_token.cancel();
    });

    // 5. Run the service
    if let Err(e) = startup::run(config, service_token).await {
        error!("❌ Notification Service failed: {:#}", e);
        std::process::exit(1);
    }

    info!("👋 Shutdown complete.");
}

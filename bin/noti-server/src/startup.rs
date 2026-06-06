//! Service startup and dependency wiring.
//!
//! Instantiates concrete adapters, injects them as trait objects into
//! the orchestrator, and starts all servers / consumers.

use std::sync::Arc;

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use noti_api::grpc::NotificationGrpcService;
use noti_api::websocket::ConnectionManager;
use noti_core::config::Config;
use noti_core::traits::{
    CacheTrait, MessageQueueTrait, NotificationProviderTrait, NotificationRepositoryTrait,
    TemplateEngineTrait, WebSocketRegistryTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::cache::CacheService;
use noti_persistence::messaging::kafka;
use noti_persistence::messaging::rabbitmq::RabbitMQClient;
use noti_persistence::providers::smtp::SmtpProvider;
use noti_persistence::providers::webhook::WebhookProvider;
use noti_persistence::providers::{MockEmailProvider, MockPushProvider, MockSmsProvider};
use noti_persistence::repository::NotificationRepository;
use noti_persistence::templating::TemplateEngine;
use noti_protocol::noti::NotificationServiceExt;

use crate::consumers;

/// Reset `Processing` rows untouched for longer than this (a crash mid-dispatch)
/// back to `Pending` during the boot recovery sweep.
const STUCK_PROCESSING_SECS: i64 = 300;
/// Max notifications re-dispatched per boot recovery sweep.
const RECOVERY_BATCH: i32 = 1000;

/// Runs the notification service with the given configuration.
///
/// # Errors
///
/// Returns an error if any infrastructure dependency (`PostgreSQL`, Redis,
/// `RabbitMQ`, `Kafka`) fails to connect, or if the HTTP/gRPC servers cannot
/// bind their ports.
#[allow(clippy::too_many_lines)]
pub async fn run(config: Config, token: CancellationToken) -> Result<()> {
    // -----------------------------------------------------------------------
    // 1. Infrastructure (Persistence & Messaging)
    // -----------------------------------------------------------------------

    // a) Database (PostgreSQL) - Tiered Routing
    let high_priority_pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .min_connections(config.database_min_connections)
        .acquire_timeout(std::time::Duration::from_secs(
            config.database_acquire_timeout_secs,
        ))
        .idle_timeout(std::time::Duration::from_secs(
            config.database_idle_timeout_secs,
        ))
        .connect(&config.database_url)
        .await
        .context("Failed to connect to High Priority PostgreSQL")?;

    let low_priority_pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections / 2)
        .min_connections(config.database_min_connections)
        .acquire_timeout(std::time::Duration::from_secs(
            config.database_acquire_timeout_secs,
        ))
        .idle_timeout(std::time::Duration::from_secs(
            config.database_idle_timeout_secs,
        ))
        .connect(&config.database_url)
        .await
        .context("Failed to connect to Low Priority PostgreSQL")?;

    info!(
        "✅ Connected to PostgreSQL with dual pools (high_max: {}, low_max: {})",
        config.database_max_connections,
        config.database_max_connections / 2
    );

    // Run migrations on high-priority pool
    sqlx::migrate!("../../migrations")
        .run(&high_priority_pool)
        .await
        .context("Failed to run database migrations")?;

    info!("✅ Database migrations completed");

    // b) Cache (Redis)
    let cache_service = Arc::new(
        CacheService::new(&config.redis_url, "noti")
            .await
            .context("Failed to create Redis client")?,
    );
    info!("✅ Redis cache service connected");

    // c) Message Queue (RabbitMQ) — REQUIRED for durable retries.
    // The in-process retry fallback loses scheduled retries on restart, so a
    // durable broker is mandatory; refuse to start without it.
    if config.rabbitmq_url.is_empty() {
        anyhow::bail!(
            "RABBITMQ_URL is required: durable retry queue cannot start without a broker"
        );
    }
    let mq_client = Some(Arc::new(
        RabbitMQClient::new(&config.rabbitmq_url)
            .await
            .context("Failed to connect to RabbitMQ")?,
    ));
    info!("✅ RabbitMQ connected");

    let mq = mq_client.clone().map(|c| c as Arc<dyn MessageQueueTrait>);

    // -----------------------------------------------------------------------
    // 2. Adapters (Providers & Repositories)
    // -----------------------------------------------------------------------

    let noti_repo = Arc::new(NotificationRepository::new(
        high_priority_pool.clone(),
        low_priority_pool.clone(),
    ));
    let template_engine = Arc::new(TemplateEngine::new("./templates")?);

    // Email Provider
    let email_provider: Arc<dyn NotificationProviderTrait> = if let Some(host) = &config.smtp_host {
        Arc::new(SmtpProvider::new(
            host,
            config.smtp_port.unwrap_or(587),
            config.smtp_user.clone(),
            config.smtp_pass.clone(),
            config
                .smtp_from
                .clone()
                .unwrap_or_else(|| "no-reply@gridtokenx.com".to_string()),
            config.smtp_tls_mode.as_deref(),
        )?)
    } else {
        warn!("⚠️ No SMTP host configured, using MockEmailProvider");
        Arc::new(MockEmailProvider)
    };

    // SMS / Push remain mocks (local capture sink); Webhook is a real HTTP POST.
    let sms_provider = Arc::new(MockSmsProvider);
    let push_provider = Arc::new(MockPushProvider);
    let webhook_provider = Arc::new(WebhookProvider::new());

    let ws_manager = Arc::new(ConnectionManager::new());
    let websocket_provider = Arc::new(
        noti_persistence::providers::websocket::WebSocketProvider::new(
            ws_manager.clone() as Arc<dyn WebSocketRegistryTrait>
        ),
    );

    // -----------------------------------------------------------------------
    // 3. Logic (Orchestrator)
    // -----------------------------------------------------------------------

    let orchestrator = Arc::new(NotificationOrchestrator::new(
        noti_repo.clone() as Arc<dyn NotificationRepositoryTrait>,
        template_engine.clone() as Arc<dyn TemplateEngineTrait>,
        email_provider,
        sms_provider as Arc<dyn NotificationProviderTrait>,
        push_provider as Arc<dyn NotificationProviderTrait>,
        webhook_provider as Arc<dyn NotificationProviderTrait>,
        websocket_provider as Arc<dyn NotificationProviderTrait>,
        cache_service.clone() as Arc<dyn CacheTrait>,
        mq,
    ));

    // -----------------------------------------------------------------------
    // 4. Background Consumers
    // -----------------------------------------------------------------------

    // a) Kafka Consumer (for IAM events)
    if !config.kafka_brokers.is_empty() {
        let kafka_consumer = kafka::create_consumer(&config.kafka_brokers, "noti-service-group")?;
        let topics = vec![
            config.kafka_topic_user_events.clone(),
            config.kafka_topic_audit_events.clone(),
        ];
        let orch = orchestrator.clone();
        let frontend_url = config.frontend_url.clone();
        let t = token.clone();
        tokio::spawn(async move {
            if let Err(e) =
                consumers::start_kafka_consumer(kafka_consumer, topics, orch, frontend_url, t).await
            {
                error!("Kafka consumer failed: {}", e);
            }
        });
    }

    // b) RabbitMQ Consumer (for background dispatch)
    if let Some(client) = mq_client {
        let orch = orchestrator.clone();
        let t = token.clone();
        tokio::spawn(async move {
            if let Err(e) = consumers::start_rabbitmq_consumer(client, orch, t).await {
                error!("RabbitMQ consumer failed: {}", e);
            }
        });
    }

    // -----------------------------------------------------------------------
    // 4b. Crash recovery sweep
    // -----------------------------------------------------------------------
    // Re-dispatch notifications a previous crash left undelivered: rows stuck
    // in `Processing` are reset to `Pending`, then all currently-due `Pending`
    // rows are re-published to the (durable) dispatch queue.
    match orchestrator
        .recover_pending(STUCK_PROCESSING_SECS, RECOVERY_BATCH)
        .await
    {
        Ok(0) => info!("✅ Crash recovery: no pending notifications to re-dispatch"),
        Ok(n) => info!("✅ Crash recovery: re-dispatched {n} pending notification(s)"),
        Err(e) => warn!("⚠️ Crash recovery sweep failed: {e}"),
    }

    // -----------------------------------------------------------------------
    // 5. API Layer (Axum)
    // -----------------------------------------------------------------------

    let grpc_service = NotificationGrpcService::new(orchestrator.clone());
    let mut router = connectrpc::Router::new();
    router = Arc::new(grpc_service).register(router);

    let grpc_router = router.into_axum_router();

    let state = noti_api::AppState {
        orchestrator: orchestrator.clone(),
    };

    let axum_router = grpc_router
        .nest(
            "/api/v1/noti",
            axum::Router::new()
                .route(
                    "/",
                    axum::routing::get(noti_api::handlers::list_notifications),
                )
                .route(
                    "/{id}",
                    axum::routing::patch(noti_api::handlers::mark_notification_as_read),
                )
                .route(
                    "/read-all",
                    axum::routing::post(noti_api::handlers::mark_all_notifications_as_read),
                )
                .with_state(state),
        )
        .route("/ws", axum::routing::get(noti_api::websocket::ws_handler))
        .route(
            "/health",
            axum::routing::get(noti_api::handlers::health_check),
        )
        .route(
            "/health/live",
            axum::routing::get(noti_api::handlers::health_live),
        )
        .layer(axum::Extension(ws_manager))
        .layer(axum::Extension(noti_api::websocket::JwtSecret(
            config.jwt_secret.clone(),
        )));

    // -----------------------------------------------------------------------
    // 6. Servers
    // -----------------------------------------------------------------------

    let port = config.port;
    let http_addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse()?;

    let grpc_port = config.grpc_port.unwrap_or(config.port + 10);
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{grpc_port}").parse()?;

    let run_token = token.clone();
    let tcp_handle = tokio::spawn(async move {
        // HTTP Server
        let http_listener = match tokio::net::TcpListener::bind(http_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind HTTP listener: {}", e);
                return;
            }
        };

        // gRPC Server
        let grpc_listener = match tokio::net::TcpListener::bind(grpc_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind gRPC listener: {}", e);
                return;
            }
        };

        let http_token = run_token.clone();
        let http_router = axum_router.clone();
        let http_srv = tokio::spawn(async move {
            info!("🚀 REST Server running on http://{}", http_addr);
            if let Err(e) = axum::serve(http_listener, http_router)
                .with_graceful_shutdown(token_to_future(http_token))
                .await
            {
                error!("HTTP server failed: {}", e);
            }
        });

        let grpc_token = run_token.clone();
        let grpc_srv = tokio::spawn(async move {
            info!("🚀 gRPC Server running on http://{}", grpc_addr);
            if let Err(e) = axum::serve(grpc_listener, axum_router)
                .with_graceful_shutdown(token_to_future(grpc_token))
                .await
            {
                error!("gRPC server failed: {}", e);
            }
        });

        tokio::select! {
            res = http_srv => { let _ = res; info!("HTTP server stopped"); }
            res = grpc_srv => { let _ = res; info!("gRPC server stopped"); }
        }
    });

    // Wait for shutdown
    token.cancelled().await;
    info!("Service cancellation received");

    // Cleanup
    tcp_handle.abort();

    Ok(())
}

async fn token_to_future(token: CancellationToken) {
    token.cancelled().await;
}

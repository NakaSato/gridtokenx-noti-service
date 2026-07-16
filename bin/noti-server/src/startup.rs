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
    CacheTrait, DeviceTokenRepositoryTrait, MessageQueueTrait, NotificationProviderTrait,
    NotificationRepositoryTrait, TemplateEngineTrait, WebSocketRegistryTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::cache::CacheService;
use noti_persistence::device_tokens::PgDeviceTokenRepository;
use noti_persistence::messaging::rabbitmq::RabbitMQClient;
use noti_persistence::providers::fcm::FcmProvider;
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
    // Idempotent re-install: main() already did this as early as possible, but
    // callers that drive run() directly (integration tests) rely on it here so
    // the /metrics route below is never wired against a no-op recorder.
    crate::metrics::install_recorder();

    // -----------------------------------------------------------------------
    // 1. Infrastructure (Persistence & Messaging)
    // -----------------------------------------------------------------------

    // Run migrations on a dedicated single-connection pool pointed at
    // migration_database_url (a session-mode pooler alias in prod). The
    // migration's pg_advisory_lock is session-scoped — running it on a pool
    // shared with regular app traffic risks the pooler handing the locked
    // connection to another client under transaction-mode pooling, leaking the
    // lock forever. Closed immediately after; the tiered pools below never
    // touch this connection.
    let migration_database_url = config
        .migration_database_url
        .clone()
        .unwrap_or_else(|| config.database_url.clone());
    let migration_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&migration_database_url)
        .await
        .context("Failed to connect to PostgreSQL for migrations")?;

    sqlx::migrate!("../../migrations")
        .run(&migration_pool)
        .await
        .context("Failed to run database migrations")?;

    info!("✅ Database migrations completed");
    migration_pool.close().await;

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

    // Push device-token registry (backs the FCM fan-out + registration API).
    let device_repo = Arc::new(PgDeviceTokenRepository::new(high_priority_pool.clone()));

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

    // SMS remains a mock (local capture sink); Webhook is a real HTTP POST.
    let sms_provider = Arc::new(MockSmsProvider);

    // Push: real FCM HTTP v1 when a project id + service-account JSON are
    // configured, else the mock sink (same degradation pattern as SMTP).
    let push_provider: Arc<dyn NotificationProviderTrait> = if let (Some(project_id), Some(creds_path)) =
        (&config.fcm_project_id, &config.fcm_credentials_path)
    {
        match FcmProvider::from_credentials_file(
            project_id.clone(),
            creds_path,
            device_repo.clone() as Arc<dyn DeviceTokenRepositoryTrait>,
        ) {
            Ok(p) => {
                // Mint a token now so bad credentials surface at boot, not on
                // the first push. Non-fatal: a transient token-endpoint blip
                // shouldn't stop the service starting — sends mint/retry later.
                match p.preflight().await {
                    Ok(()) => info!("✅ FCM push provider configured + validated (project {project_id})"),
                    Err(e) => warn!(
                        "⚠️ FCM provider configured (project {project_id}) but preflight token mint failed ({e}); will retry on send"
                    ),
                }
                Arc::new(p)
            }
            Err(e) => {
                warn!("⚠️ FCM configured but failed to initialize ({e}); using MockPushProvider");
                Arc::new(MockPushProvider)
            }
        }
    } else {
        warn!("⚠️ No FCM project/credentials configured, using MockPushProvider");
        Arc::new(MockPushProvider)
    };

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
        push_provider,
        webhook_provider as Arc<dyn NotificationProviderTrait>,
        websocket_provider as Arc<dyn NotificationProviderTrait>,
        cache_service.clone() as Arc<dyn CacheTrait>,
        mq,
    ));

    // -----------------------------------------------------------------------
    // 4. Background Consumers
    // -----------------------------------------------------------------------

    // a) Kafka Consumer (for IAM events)
    let kafka_health = Arc::new(noti_core::health::KafkaConsumerHealth::new());
    if config.kafka_brokers.is_empty() {
        // No brokers configured: the consumer never runs, so readiness must not
        // claim a live consumer it doesn't have — mark it disabled (ready OK).
        kafka_health.mark_disabled();
    } else {
        let brokers = config.kafka_brokers.clone();
        let topics = vec![
            config.kafka_topic_user_events.clone(),
            // Carries `VerificationEmailRequested` — IAM routes it via its
            // catch-all topic arm (event_bus/kafka.rs). Do NOT drop this
            // subscription or verification emails stop arriving.
            config.kafka_topic_audit_events.clone(),
            config.kafka_topic_trading_triggers.clone(),
        ];
        let orch = orchestrator.clone();
        let frontend_url = config.frontend_url.clone();
        let health_for_consumer = kafka_health.clone();
        let t = token.clone();
        tokio::spawn(async move {
            if let Err(e) = consumers::start_kafka_consumer(
                brokers,
                "noti-service-group".to_string(),
                topics,
                orch,
                frontend_url,
                health_for_consumer,
                t,
            )
            .await
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

    let grpc_service = NotificationGrpcService::new(orchestrator.clone(), config.jwt_secret.clone());
    let mut router = connectrpc::Router::new();
    router = Arc::new(grpc_service).register(router);

    let grpc_router = router.into_axum_router();

    let state = noti_api::AppState {
        orchestrator: orchestrator.clone(),
        device_repo: device_repo.clone() as Arc<dyn DeviceTokenRepositoryTrait>,
    };

    // Register the notification REST routes with explicit absolute paths and merge them,
    // rather than `nest("/api/v1/noti", child-with-"/"-route)`. Nesting a router whose
    // collection handler sits at the "/" child path leaves `GET /api/v1/noti[/]` shadowed
    // by the ConnectRPC catch-all (returns `unimplemented`), even though `/{id}` and
    // `/read-all` resolve. Both slash variants are bound so trailing-slash clients work.
    let noti_rest = axum::Router::new()
        .route(
            "/api/v1/noti",
            axum::routing::get(noti_api::handlers::list_notifications),
        )
        .route(
            "/api/v1/noti/",
            axum::routing::get(noti_api::handlers::list_notifications),
        )
        .route(
            "/api/v1/noti/{id}",
            axum::routing::patch(noti_api::handlers::mark_notification_as_read),
        )
        .route(
            "/api/v1/noti/read-all",
            axum::routing::post(noti_api::handlers::mark_all_notifications_as_read),
        )
        .route(
            "/api/v1/noti/devices",
            axum::routing::get(noti_api::handlers::list_devices)
                .post(noti_api::handlers::register_device),
        )
        .route(
            "/api/v1/noti/devices/{token}",
            axum::routing::delete(noti_api::handlers::revoke_device),
        )
        .with_state(state);

    let axum_router = grpc_router
        .merge(noti_rest)
        .route("/ws", axum::routing::get(noti_api::websocket::ws_handler))
        .route(
            "/health",
            axum::routing::get(noti_api::handlers::health_check),
        )
        .route(
            "/health/live",
            axum::routing::get(noti_api::handlers::health_live),
        )
        .route(
            "/health/ready",
            axum::routing::get(noti_api::handlers::health_ready),
        )
        .route("/metrics", axum::routing::get(metrics_handler))
        .layer(axum::Extension(kafka_health.clone()))
        .layer(axum::Extension(ws_manager))
        .layer(axum::Extension(noti_api::websocket::JwtSecret(
            config.jwt_secret.clone(),
        )))
        // Trace + count every HTTP request. Outermost layers so they wrap the
        // merged REST + ConnectRPC routers.
        .layer(axum::middleware::from_fn(crate::metrics::track_http))
        // INFO-level request span so traces export to Tempo (the default
        // make_span is DEBUG and is filtered out under the standard `info` env).
        .layer(tower_http::trace::TraceLayer::new_for_http().make_span_with(
            |request: &axum::http::Request<axum::body::Body>| {
                tracing::info_span!(
                    "http_request",
                    method = %request.method(),
                    uri = %request.uri().path(),
                )
            },
        ));

    // -----------------------------------------------------------------------
    // 6. Servers
    // -----------------------------------------------------------------------

    let port = config.port;
    let http_addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse()?;

    let grpc_port = config.grpc_port.unwrap_or(config.port + 10);
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{grpc_port}").parse()?;

    let run_token = token.clone();
    let mut tcp_handle = tokio::spawn(async move {
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
        // Swagger UI at /swagger-ui, raw spec at /api-docs/openapi.json.
        // Merged onto the HTTP router only — the gRPC/ConnectRPC port never
        // serves the browser docs UI.
        let openapi = <noti_api::openapi::ApiDoc as utoipa::OpenApi>::openapi();
        let swagger =
            utoipa_swagger_ui::SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi);
        let http_router = axum_router.clone().merge(swagger);
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

        // Both servers exit on their own once the token cancels and their
        // in-flight requests drain (`with_graceful_shutdown`).
        let _ = http_srv.await;
        info!("HTTP server stopped");
        let _ = grpc_srv.await;
        info!("gRPC server stopped");
    });

    // Wait for shutdown
    token.cancelled().await;
    info!("Service cancellation received");

    // Graceful drain: the servers watch the same token, so wait for them to
    // finish their in-flight requests rather than aborting them mid-response.
    // A deadline caps how long a hung request can stall shutdown.
    if tokio::time::timeout(std::time::Duration::from_secs(30), &mut tcp_handle)
        .await
        .is_err()
    {
        error!("Servers did not drain within 30s; aborting");
        tcp_handle.abort();
    }

    Ok(())
}

async fn token_to_future(token: CancellationToken) {
    token.cancelled().await;
}

/// Serves Prometheus metrics in text-exposition format. Gated to internal CIDRs
/// at the APISIX gateway (same policy as `/health`).
async fn metrics_handler() -> String {
    crate::metrics::render()
}

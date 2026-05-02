//! Service startup and dependency wiring.
//!
//! Instantiates concrete adapters, injects them as trait objects into
//! the orchestrator, and starts all servers / consumers.

use std::io::BufReader;
use std::sync::Arc;

use anyhow::{Context, Result};
use quinn::Endpoint;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use noti_api::grpc::NotificationGrpcService;
use noti_core::config::Config;
use noti_core::traits::{
    CacheTrait, MessageQueueTrait, NotificationProviderTrait, NotificationRepositoryTrait,
    TemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::cache::CacheService;
use noti_persistence::providers::{
    MockEmailProvider, MockPushProvider, MockSmsProvider, MockWebhookProvider,
};
use noti_persistence::repository::NotificationRepository;
use noti_persistence::templating::TemplateEngine;
use noti_protocol::noti::NotificationServiceExt;

use crate::consumers;

const ALPN_H3: &[u8] = b"h3";

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    Ok(rustls_pemfile::certs(&mut reader).collect::<std::io::Result<Vec<_>>>()?)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    let mut keys = Vec::new();
    
    // Try different key formats
    for result in rustls_pemfile::read_all(&mut reader) {
        match result? {
            rustls_pemfile::Item::Pkcs8Key(key) => keys.push(PrivateKeyDer::Pkcs8(key)),
            rustls_pemfile::Item::Pkcs1Key(key) => keys.push(PrivateKeyDer::Pkcs1(key)),
            rustls_pemfile::Item::Sec1Key(key) => keys.push(PrivateKeyDer::Sec1(key)),
            _ => continue,
        }
    }

    if let Some(key) = keys.into_iter().next() {
        Ok(key)
    } else {
        anyhow::bail!("No supported private keys found in key file")
    }
}

pub async fn run(config: Config, token: CancellationToken) -> Result<()> {
    // -----------------------------------------------------------------------
    // 1. Database
    // -----------------------------------------------------------------------
    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    sqlx::migrate!("../../migrations")
        .run(&db_pool)
        .await
        .context("Failed to run database migrations")?;
    info!("✅ Database migrations completed");

    // -----------------------------------------------------------------------
    // 2. Infrastructure adapters → trait objects
    // -----------------------------------------------------------------------
    let cache: Arc<dyn CacheTrait> = Arc::new(
        CacheService::new(&config.redis_url, "noti")
            .await
            .context("Failed to connect to Redis")?,
    );

    let repo: Arc<dyn NotificationRepositoryTrait> =
        Arc::new(NotificationRepository::new(db_pool.clone()));

    let template_engine: Arc<dyn TemplateEngineTrait> = Arc::new(
        TemplateEngine::new("templates").context("Failed to initialize template engine")?,
    );

    // WebSocket management
    let ws_manager = Arc::new(noti_api::websocket::ConnectionManager::new());
    let ws_registry: Arc<dyn noti_core::traits::WebSocketRegistryTrait> = ws_manager.clone();

    let email_provider: Arc<dyn NotificationProviderTrait> = if let Some(host) = config.smtp_host.as_ref() {
        info!("📧 Configuring real SMTP provider for {}", host);
        Arc::new(noti_persistence::providers::smtp::SmtpProvider::new(
            host,
            config.smtp_port.unwrap_or(587),
            config.smtp_user.clone(),
            config.smtp_pass.clone(),
            config.smtp_from.clone().unwrap_or_else(|| "no-reply@gridtokenx.com".to_string()),
        ))
    } else {
        tracing::warn!("⚠️ No SMTP host configured, using MockEmailProvider");
        Arc::new(MockEmailProvider)
    };

    let sms_provider: Arc<dyn NotificationProviderTrait> = Arc::new(MockSmsProvider);
    let push_provider: Arc<dyn NotificationProviderTrait> = Arc::new(MockPushProvider);
    let webhook_provider: Arc<dyn NotificationProviderTrait> = Arc::new(MockWebhookProvider);
    let websocket_provider: Arc<dyn NotificationProviderTrait> = Arc::new(
        noti_persistence::providers::websocket::WebSocketProvider::new(ws_registry)
    );

    // Optional RabbitMQ
    let mq_client = if !config.rabbitmq_url.is_empty() {
        match noti_persistence::messaging::rabbitmq::RabbitMQClient::new(&config.rabbitmq_url)
            .await
        {
            Ok(mq) => Some(Arc::new(mq)),
            Err(e) => {
                error!("Failed to initialize RabbitMQ client: {}", e);
                None
            }
        }
    } else {
        None
    };

    let mq: Option<Arc<dyn MessageQueueTrait>> =
        mq_client.clone().map(|c| c as Arc<dyn MessageQueueTrait>);

    // -----------------------------------------------------------------------
    // 3. Build orchestrator
    // -----------------------------------------------------------------------
    let orchestrator = Arc::new(NotificationOrchestrator::new(
        repo,
        template_engine,
        email_provider,
        sms_provider,
        push_provider,
        webhook_provider,
        websocket_provider,
        cache,
        mq,
    ));

    // -----------------------------------------------------------------------
    // 4. Start background consumers
    // -----------------------------------------------------------------------
    if let Some(mq) = mq_client.clone() {
        let orch = orchestrator.clone();
        let t = token.clone();
        tokio::spawn(async move {
            if let Err(e) = consumers::start_rabbitmq_consumer(mq, orch, t).await {
                error!("RabbitMQ consumer failed: {}", e);
            }
        });
    }

    if !config.kafka_brokers.is_empty() {
        let kafka_consumer = noti_persistence::messaging::kafka::create_consumer(
            &config.kafka_brokers,
            "noti-service-group",
        )
        .context("Failed to create Kafka consumer")?;

        let topics = vec![
            "iam.user.events".to_string(),
            "trading.trade.events".to_string(),
        ];
        let orch = orchestrator.clone();
        let t = token.clone();
        tokio::spawn(async move {
            if let Err(e) = consumers::start_kafka_consumer(kafka_consumer, topics, orch, t).await
            {
                error!("Kafka consumer failed: {}", e);
            }
        });
    }

    // -----------------------------------------------------------------------
    // 5. gRPC / ConnectRPC service
    // -----------------------------------------------------------------------
    let grpc_service = NotificationGrpcService::new(orchestrator.clone());
    let mut router = connectrpc::Router::new();
    router = Arc::new(grpc_service).register(router);

    let grpc_router = router.into_axum_router();
    
    let state = noti_api::AppState {
        orchestrator: orchestrator.clone(),
    };
    
    // -----------------------------------------------------------------------
    // 6. Build Axum Router
    // -----------------------------------------------------------------------
    
    // a) Stateful routes
    let stateful_router = axum::Router::new()
        .route("/api/v1/users/me/notifications", axum::routing::get(noti_api::handlers::list_notifications))
        .route("/api/v1/users/me/notifications/{id}", axum::routing::patch(noti_api::handlers::mark_notification_as_read))
        .route("/api/v1/users/me/notifications/mark-all-read", axum::routing::post(noti_api::handlers::mark_all_notifications_as_read))
        .with_state(state);

    // b) Combine everything into the final Router<()>
    let axum_router = axum::Router::new()
        .nest("/", grpc_router)
        .nest("/", stateful_router)
        .route("/ws", axum::routing::get(noti_api::websocket::ws_handler))
        .route("/health", axum::routing::get(noti_api::handlers::health_check))
        .route("/health/live", axum::routing::get(noti_api::handlers::health_live))
        .layer(axum::Extension(ws_manager))
        .layer(axum::Extension(noti_api::websocket::JwtSecret(config.jwt_secret.clone())));

    // Add Alt-Svc header for HTTP/3 advertisement
    let axum_router = axum_router.layer(
        tower_http::set_header::SetResponseHeaderLayer::overriding(
            http::header::ALT_SVC,
            http::HeaderValue::from_static("h3=\":5060\"; ma=86400"),
        ),
    );

    let http_addr: std::net::SocketAddr = format!("0.0.0.0:{}", config.port)
        .parse()
        .context("Failed to parse HTTP address")?;

    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", config.port + 10)
        .parse()
        .context("Failed to parse gRPC address")?;

    info!(
        "🚀 Notification HTTP Service starting on {} (TCP)",
        http_addr
    );
    info!(
        "🚀 Notification gRPC Service starting on {} (TCP/UDP)",
        grpc_addr
    );

    // -----------------------------------------------------------------------
    // 5a. HTTP/3 (QUIC) server (on gRPC port)
    // -----------------------------------------------------------------------
    let h3_token = token.clone();
    let h3_router = axum_router.clone();
    let h3_handle = tokio::spawn(async move {
        let cert_file =
            std::env::var("CERT_FILE").unwrap_or_else(|_| "infra/certs/server.crt".to_string());
        let key_file =
            std::env::var("KEY_FILE").unwrap_or_else(|_| "infra/certs/server.key".to_string());

        let certs = match load_certs(&cert_file) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to load certs for H3: {}. H3 server disabled.", e);
                return;
            }
        };
        let key = match load_private_key(&key_file) {
            Ok(k) => k,
            Err(e) => {
                error!("Failed to load key for H3: {}. H3 server disabled.", e);
                return;
            }
        };

        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut rustls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("Failed to create TLS config");
        rustls_config.alpn_protocols = vec![ALPN_H3.to_vec()];

        let quic_crypto = match quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create QUIC crypto config: {}", e);
                return;
            }
        };

        let endpoint = match Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(quic_crypto)),
            grpc_addr,
        ) {
            Ok(e) => e,
            Err(e) => {
                error!("Failed to start QUIC endpoint: {}", e);
                return;
            }
        };

        loop {
            tokio::select! {
                incoming = endpoint.accept() => {
                    if let Some(conn) = incoming {
                        let router = h3_router.clone();
                        tokio::spawn(async move {
                            match conn.await {
                                Ok(conn) => {
                                    let mut h3_conn = match h3::server::Connection::new(
                                        h3_quinn::Connection::new(conn),
                                    )
                                    .await
                                    {
                                        Ok(c) => c,
                                        Err(e) => {
                                            error!("Failed to create H3 connection: {}", e);
                                            return;
                                        }
                                    };

                                    loop {
                                        match h3_conn.accept().await {
                                            Ok(Some(resolver)) => {
                                                let router = router.clone();
                                                tokio::spawn(async move {
                                                    if let Err(e) =
                                                        h3_axum::serve_h3_with_axum(router, resolver)
                                                            .await
                                                    {
                                                        error!("H3 request failed: {}", e);
                                                    }
                                                });
                                            }
                                            Ok(None) => break,
                                            Err(e) => {
                                                if !h3_axum::is_graceful_h3_close(&e) {
                                                    error!("H3 connection error: {}", e);
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(e) => error!("QUIC connection failed: {}", e),
                            }
                        });
                    }
                }
                _ = h3_token.cancelled() => {
                    break;
                }
            }
        }
        info!("🔄 Notification H3 Service shutting down...");
    });

    // -----------------------------------------------------------------------
    // 5b. TCP servers (Axum)
    // -----------------------------------------------------------------------
    let tcp_token = token.clone();
    let http_router = axum_router.clone();
    let g_addr = grpc_addr;
    let h_addr = http_addr;
    
    let tcp_handle = tokio::spawn(async move {
        // HTTP Server
        let http_listener = match tokio::net::TcpListener::bind(h_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind HTTP listener: {}", e);
                return;
            }
        };
        
        // gRPC Server
        let grpc_listener = match tokio::net::TcpListener::bind(g_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind gRPC listener: {}", e);
                return;
            }
        };

        let http_token = tcp_token.clone();
        let http_srv = tokio::spawn(async move {
            if let Err(e) = axum::serve(http_listener, http_router)
                .with_graceful_shutdown(async move {
                    http_token.cancelled().await;
                })
                .await
            {
                error!("HTTP server failed: {}", e);
            }
        });

        let grpc_token = tcp_token.clone();
        let grpc_srv = tokio::spawn(async move {
            if let Err(e) = axum::serve(grpc_listener, axum_router)
                .with_graceful_shutdown(async move {
                    grpc_token.cancelled().await;
                })
                .await
            {
                error!("gRPC server failed: {}", e);
            }
        });

        tokio::select! {
            _ = http_srv => info!("HTTP server stopped"),
            _ = grpc_srv => info!("gRPC server stopped"),
        }
    });

    // -----------------------------------------------------------------------
    // 6. Wait for shutdown
    // -----------------------------------------------------------------------
    token.cancelled().await;
    info!("Service cancellation received");
    
    // Cleanup
    let _ = tcp_handle.abort();
    let _ = h3_handle.abort();

    Ok(())
}

//! Startup-smoke + graceful-shutdown end-to-end test (closes gap #8).
//!
//! Every other test drives one leg (a consumer, a provider, a handler) against
//! hand-assembled wiring. None boots the *actual* `startup::run` — the single
//! function that connects every dependency, runs migrations, spawns both
//! consumers, runs the crash-recovery sweep, and binds the dual HTTP + gRPC
//! servers with graceful shutdown. A regression in that wiring (a bad `bind`
//! order, a panic in recovery, a server that never comes up, or one that won't
//! stop) only surfaces here.
//!
//! This boots `run(config, token)` against real Postgres + Redis + `RabbitMQ`
//! (Kafka left unconfigured → the consumer is marked disabled, so readiness
//! still passes), then:
//!   1. polls the live `/health` endpoint until it answers `200` — proving the
//!      HTTP server bound and the whole graph assembled without error,
//!   2. confirms `/health/ready` is `200` (consumer-health wiring + the
//!      disabled-Kafka readiness path),
//!   3. TCP-connects the gRPC port to prove the second server bound too,
//!   4. cancels the token and asserts `run` returns `Ok(())` and the HTTP port
//!      stops accepting — i.e. graceful shutdown actually tears the servers down.
//!
//! Requires a Docker/OrbStack daemon — panics (does not skip) without one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use noti_core::config::Config;
use noti_server::startup;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;

/// Grab a currently-free TCP port by binding to `:0` and immediately releasing
/// it. Racy in theory; fine for a single heavyweight container test.
async fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.expect("bind :0");
    let port = l.local_addr().expect("local addr").port();
    drop(l);
    port
}

/// Build a `Config` pointing at the booted containers, Kafka disabled.
#[allow(clippy::too_many_arguments)]
fn test_config(
    port: u16,
    grpc_port: u16,
    database_url: String,
    redis_url: String,
    rabbitmq_url: String,
) -> Config {
    Config {
        port,
        database_url,
        kafka_brokers: String::new(), // empty → consumer disabled (ready still OK)
        rabbitmq_url,
        redis_url,
        log_level: "info".to_string(),
        database_max_connections: 5,
        database_min_connections: 1,
        database_acquire_timeout_secs: 30,
        database_idle_timeout_secs: 600,
        twilio_account_sid: None,
        twilio_auth_token: None,
        fcm_project_id: None,
        fcm_credentials_path: None,
        smtp_host: None, // → MockEmailProvider
        smtp_port: None,
        smtp_user: None,
        smtp_pass: None,
        smtp_from: None,
        smtp_tls_mode: None,
        jwt_secret: "startup-smoke-secret".to_string(),
        frontend_url: Some("https://app.gridtokenx.test".to_string()),
        grpc_port: Some(grpc_port),
        kafka_topic_user_events: "iam.user.events".to_string(),
        kafka_topic_audit_events: "iam.audit.events".to_string(),
        kafka_topic_trading_triggers: "trading.triggers".to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_boots_serves_health_then_shuts_down_cleanly() {
    // `startup::run` loads templates from the cwd-relative `./templates`. Under
    // `cargo test` the cwd is the crate dir (`bin/noti-server`); point it at the
    // service root so the real template dir resolves, exactly as the binary
    // sees it when launched from there.
    let service_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::env::set_current_dir(&service_root).expect("chdir to service root");

    // ---- Boot the three required dependencies ----------------------------
    let pg = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres");
    let redis = Redis::default().start().await.expect("start redis");
    let rabbit = RabbitMq::default().start().await.expect("start rabbitmq");

    let pg_host = pg.get_host().await.expect("pg host");
    let pg_port = pg.get_host_port_ipv4(5432).await.expect("pg port");
    let database_url = format!("postgres://postgres:postgres@{pg_host}:{pg_port}/postgres");

    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    let rb_host = rabbit.get_host().await.expect("rabbit host");
    let rb_port = rabbit.get_host_port_ipv4(5672).await.expect("rabbit port");
    let rabbitmq_url = format!("amqp://guest:guest@{rb_host}:{rb_port}");

    let http_port = free_port().await;
    let grpc_port = free_port().await;
    let config = test_config(http_port, grpc_port, database_url, redis_url, rabbitmq_url);

    // ---- Boot the real service -------------------------------------------
    let token = CancellationToken::new();
    let run_handle = tokio::spawn(startup::run(config, token.clone()));

    let client = reqwest::Client::new();
    let health_url = format!("http://127.0.0.1:{http_port}/health");
    let ready_url = format!("http://127.0.0.1:{http_port}/health/ready");

    // 1. Poll /health until the HTTP server answers 200 (graph fully assembled).
    let up = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if client
                .get(&health_url)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(up.is_ok(), "/health never returned 200 — startup wiring broke");

    // 2. Readiness is 200 (consumer-health Extension wired; Kafka disabled path).
    let ready = client.get(&ready_url).send().await.expect("ready request");
    assert!(
        ready.status().is_success(),
        "/health/ready must be 200 with Kafka disabled, got {}",
        ready.status()
    );

    // 3. The gRPC server bound its own port (PORT+offset).
    tokio::net::TcpStream::connect(("127.0.0.1", grpc_port))
        .await
        .expect("gRPC port must be accepting connections");

    // ---- Graceful shutdown -----------------------------------------------
    token.cancel();

    // `run` returns only after cancellation; it must return Ok(()).
    let run_result = tokio::time::timeout(Duration::from_secs(15), run_handle)
        .await
        .expect("run did not return within 15s of cancellation")
        .expect("run task panicked");
    assert!(run_result.is_ok(), "run returned an error: {run_result:?}");

    // The HTTP port must stop accepting once the servers are torn down.
    let stopped = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client.get(&health_url).send().await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(stopped.is_ok(), "HTTP port still accepting after shutdown");
}

//! Push-channel full-pipeline end-to-end test (closes gap #1 for Push).
//!
//! `full_pipeline.rs` proves the Email channel end to end; `websocket_flow.rs`
//! and `webhook_dispatch.rs` cover WebSocket and Webhook. The **Push (FCM)**
//! channel — the newest, with the most moving parts (a device-token registry
//! fan-out and a self-minted `OAuth2` send) — had no test driving it through the
//! assembled graph. A regression in the Kafka→Push wiring (wrong channel→provider
//! map, the device fan-out, the `push_notification.txt.tera` render, or the FCM
//! send leg) would slip past every single-leg test.
//!
//! This boots the four real brokers/stores (Kafka, Postgres, Redis, `RabbitMQ`),
//! assembles the production graph exactly as `startup::run` does (minus the
//! HTTP/gRPC servers) with the **real `FcmProvider`** as the push provider —
//! pointed at a local HTTP stub standing in for Google's token + `messages:send`
//! endpoints — backed by the **real `PgDeviceTokenRepository`** over Postgres.
//! It seeds one device token for a user, produces a real `UserWalletLinked`
//! event (a security alert that fans out to WebSocket *and* Push), and drives it
//! the whole way:
//!
//!   produce `UserWalletLinked` to Kafka
//!     → `start_kafka_consumer` → `orchestrator.queue_notification` (push row)
//!       → Redis idempotency + Postgres insert (`pending`)
//!         → `RabbitMQ` `noti.dispatch` → `start_rabbitmq_consumer`
//!           → `orchestrator.dispatch` → Tera render → `FcmProvider.send`
//!             → device-registry fan-out → FCM stub `messages:send` (200)
//!               → Postgres `update_status(sent)`
//!
//! Terminal proof: the **push** row for the user reaches `sent` (written only
//! after `FcmProvider` returned `Ok`), and the stub actually received a
//! `messages:send` carrying the seeded device token and the minted bearer.
//!
//! Requires a Docker/OrbStack daemon — panics (does not skip) without one.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::duration_suboptimal_units
)]

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use noti_core::domain::DevicePlatform;
use noti_core::health::KafkaConsumerHealth;
use noti_core::traits::{
    CacheTrait, DeviceTokenRepositoryTrait, MessageQueueTrait, NotificationProviderTrait,
    NotificationRepositoryTrait, TemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::cache::CacheService;
use noti_persistence::device_tokens::PgDeviceTokenRepository;
use noti_persistence::messaging::rabbitmq::RabbitMQClient;
use noti_persistence::providers::fcm::FcmProvider;
use noti_persistence::providers::{MockSmsProvider, MockWebSocketProvider, MockWebhookProvider};
use noti_persistence::repository::NotificationRepository;
use noti_persistence::templating::TemplateEngine;
use noti_server::consumers::{start_kafka_consumer, start_rabbitmq_consumer};

use testcontainers_modules::kafka::apache;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;

mod common;
use common::{kafka_brokers, pg_url, rabbit_url, redis_url};

const TOPIC: &str = "iam.user.events";

/// Throwaway PKCS#8 RSA key (test-only) that signs the JWT assertion the stub's
/// `/token` endpoint accepts. Grants access to nothing real.
const TEST_RSA_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDcpW8kTQiUoJSY\ngykivVmKUB6n/cnv9AZDhpAnOtjg2JldACT87AS2x0jZxbNeiQBRpPahKT2UPOnY\noni3y3yw6kV36Hwkxscr+KvUNVHu7XYORcF0ehfYRI0VCLZWWe0V5bsk0Dj82yYW\nvYLOld8KbwfcIFrD3oidWFdj99xdfPI9mSMi+VGOFR5+yePGJZKDNbOS1O1aLzQ0\nnt3KY8E+RpeUlLxvWju019ljUoWp4X7CzTeI5CDkXrPWLZ3ibRPDmZWXfUVTe83W\n3qT4ThY7kEpZhB9ZEvbBUTjWs5XNjawwMidZ+Efc5YxPJQZcmWtSckAi8pzEmH+N\nLLZxZKmjAgMBAAECggEAT3M4ioM8kDwkVaiA8vPono/MAiS2BrPBX6ZAGQgtGQWV\nb7ICH5qi9efbeSMhu+wsE7oJXq25cTvf3nRecJwSqaep3Qv3S8zR4ij4QoDyoEyc\nQnZmuxjNpj/E52qMMZrO7qAa254orxAAbpbN17KKrjidxWtXE4l5euLZEPOqw3Rz\nkMKvgwfYhgzl2ASkYcPdmOBGqxaiGZ+84ZzcKmATNuPimwcNqx8e1m7UNtIJfkj9\no6zHLaC86XMh4nWiFS7ilebrj7SoR5jz+Wyzy7Xcwmux1xJTO5nc2ceAK42kXIhO\nQWa7DXfhrE3p4x7vsY+0qIDdnyKqivazCntMLen9yQKBgQD0j/dX0GjCpayHVCes\n+IIxNeN5wLd96MIn9MaX+/dENxB1jH73wQ/YIZLLh9hOjp0OsMWQvagF7fPovTsV\ndNQrt+Tb6sOBq+tfOfpyhoY9Egi9QqcjWpGnErjNbeGg3zkAQExDxsgbGFjs6Nha\no3pzIY4azFtGhcS0P5HGHfjlSQKBgQDm9yGDDiQ7xM5IPIAt2WDuZrk3uoGSkQtu\nRPqSTg97F5dQ7HP83Incpp770hdNyrsZ3od6Esulx2JxK7WFwYvIXbOdHvs7pgg2\nC+jPf9weTiR6Q8+Lrb1YbfPwxUPBN0nEIFA9QSIt4hO3zVkT4bMWdH287MDmTrIO\nbfTGpR3TiwKBgGCki4+uEdfpdFY+ETevNHOR4gR4/YnJ8v+rINdqgHn6cIyjKoFp\nT4OPMN0xH29buADYJhpeeAlv0NUGAlUmR7nG/69QBFY3w9lrpeaf9mgnukBgGIBG\nCAzHvzOe2myiCXpp7jlSUj0yz+E+2lBnDbp1Zhx86QzjS6oW/NoXegXRAoGBALmp\nayz4jzPkjpYO3FL+7SZ3OOiNal8xbWjk1jAJw/QFEMQib1KSzdersR1o0wbbsu+m\nrGz68u1+i6nBoxe0b/NPL3VcVESswOkBRdKXS5Co7DXEkPANZ6nQKUogqMiG8ytP\ndnDnDNypYYRc9ABBbD7ewby+7Im2NPfYd+2/CWzlAoGBAK5Xm/8KMVMRedZJTEPX\nXbGb6ZioTOnMHPNoTN3PyP+l4Mp7kEG0eBg8yifldC6vk1MXLa8V40cmSwKUK84L\nlYAEIongiTh9yXNqfLIAq5rViZYRUFTlYKwr2Esks5zyDpeUGPjYw9ibFvZmDpnq\nDRmbNXHyveNnaAgPb2zhReeB\n-----END PRIVATE KEY-----\n";

/// One captured `messages:send` request body + whether it carried the bearer.
#[derive(Clone)]
struct SendCapture {
    body: String,
    had_bearer: bool,
}

/// A blocking std-TCP stub for the FCM token + `messages:send` endpoints, run on
/// its own thread (the workspace `tokio` has no `net` feature, so no async
/// listener here). Always answers `200` — this test exercises the happy path.
fn spawn_fcm_stub() -> (String, Arc<Mutex<Vec<SendCapture>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fcm stub");
    let port = listener.local_addr().expect("local addr").port();
    let sends: Arc<Mutex<Vec<SendCapture>>> = Arc::new(Mutex::new(Vec::new()));

    let srv_sends = sends.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let Some((head, body)) = read_http_request(&mut stream) else {
                continue;
            };
            let request_line = head.lines().next().unwrap_or_default();

            let response = if request_line.contains("/token") {
                let json = r#"{"access_token":"stub-access-token","expires_in":3600}"#;
                http_response(json)
            } else if request_line.contains("messages:send") {
                let had_bearer = head
                    .to_lowercase()
                    .contains("authorization: bearer stub-access-token");
                srv_sends
                    .lock()
                    .expect("sends lock")
                    .push(SendCapture { body, had_bearer });
                http_response(r#"{"name":"projects/p/messages/1"}"#)
            } else {
                http_response("{}")
            };

            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    (format!("http://127.0.0.1:{port}"), sends)
}

/// Read one HTTP/1.1 request (headers + `Content-Length` body) from a blocking
/// socket. Returns `(head, body)`.
fn read_http_request(stream: &mut std::net::TcpStream) -> Option<(String, String)> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];

    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let content_length = head
        .lines()
        .find_map(|l| {
            l.strip_prefix("Content-Length:")
                .or_else(|| l.strip_prefix("content-length:"))
        })
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);

    while buf.len() < header_end + content_length {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }

    let body = String::from_utf8_lossy(&buf[header_end..]).into_owned();
    Some((head, body))
}

fn http_response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Write a throwaway service-account JSON whose `token_uri` points at the stub.
fn write_service_account(token_uri: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "fcm-pipeline-sa-{}-{}.json",
        std::process::id(),
        Uuid::new_v4()
    ));
    let json = serde_json::json!({
        "type": "service_account",
        "project_id": "stub-project",
        "private_key_id": "stub-key-id",
        "private_key": TEST_RSA_PRIVATE_KEY,
        "client_email": "stub@stub-project.iam.gserviceaccount.com",
        "token_uri": token_uri,
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&json).expect("serialize SA"))
        .expect("write service-account file");
    path.to_string_lossy().into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_event_flows_end_to_end_to_sent() {
    // ---- 1. Boot the four real dependencies + the FCM stub ----------------
    let pg = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres");
    let redis = Redis::default().start().await.expect("start redis");
    let rabbit = testcontainers_modules::rabbitmq::RabbitMq::default()
        .start()
        .await
        .expect("start rabbitmq");
    let kafka = apache::Kafka::default().start().await.expect("start kafka");

    let (fcm_base_url, stub_sends) = spawn_fcm_stub();

    let pool = PgPool::connect(&pg_url(&pg).await)
        .await
        .expect("connect pg pool");
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    // ---- 2. Seed a device token for the target user ----------------------
    let user_id = Uuid::new_v4();
    let device_token = format!("dev-token-{}", Uuid::new_v4());
    let device_repo = Arc::new(PgDeviceTokenRepository::new(pool.clone()));
    device_repo
        .register(user_id, &device_token, DevicePlatform::Android)
        .await
        .expect("seed device token");

    // ---- 3. Assemble the production graph (mirrors startup::run) ----------
    let repo = NotificationRepository::new(pool.clone(), pool.clone());
    let cache = CacheService::new(&redis_url(&redis).await, "noti")
        .await
        .expect("connect cache");
    let mq_client = Arc::new(
        RabbitMQClient::new(&rabbit_url(&rabbit).await)
            .await
            .expect("connect rabbitmq"),
    );
    let templates = concat!(env!("CARGO_MANIFEST_DIR"), "/../../templates");
    let template_engine = TemplateEngine::new(templates).expect("load templates");

    // The real push provider, pointed at the stub, backed by the real registry.
    let sa_path = write_service_account(&format!("{fcm_base_url}/token"));
    let push_provider = FcmProvider::from_credentials_file(
        "stub-project".to_string(),
        &sa_path,
        device_repo.clone() as Arc<dyn DeviceTokenRepositoryTrait>,
    )
    .expect("build FcmProvider")
    .with_messaging_base_url(fcm_base_url.clone());

    let orchestrator = Arc::new(NotificationOrchestrator::new(
        Arc::new(repo) as Arc<dyn NotificationRepositoryTrait>,
        Arc::new(template_engine) as Arc<dyn TemplateEngineTrait>,
        Arc::new(MockWebhookProvider) as Arc<dyn NotificationProviderTrait>, // email (unused)
        Arc::new(MockSmsProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(push_provider) as Arc<dyn NotificationProviderTrait>, // push (REAL)
        Arc::new(MockWebhookProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockWebSocketProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(cache) as Arc<dyn CacheTrait>,
        Some(mq_client.clone() as Arc<dyn MessageQueueTrait>),
    ));

    // ---- 4. Start the production consumers --------------------------------
    let token = CancellationToken::new();
    let health = Arc::new(KafkaConsumerHealth::new());

    let kafka_consumer = tokio::spawn(start_kafka_consumer(
        kafka_brokers(&kafka).await,
        format!("noti-push-e2e-{}", Uuid::new_v4()),
        vec![TOPIC.to_string()],
        orchestrator.clone(),
        Some("https://app.gridtokenx.test".to_string()),
        health,
        token.clone(),
    ));
    let rabbit_consumer = tokio::spawn(start_rabbitmq_consumer(
        mq_client.clone(),
        orchestrator.clone(),
        token.clone(),
    ));

    // ---- 5. Produce a real UserWalletLinked event, wait for push `sent` ----
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers(&kafka).await)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("create producer");

    let payload = serde_json::json!({
        "event_type": "UserWalletLinked",
        "data": {
            "user_id": user_id.to_string(),
            "wallet_address": "Wa11etPub1icKey",
            "shard_id": 0,
            "transaction_signature": "5xSig"
        }
    })
    .to_string();

    let uid_str = user_id.to_string();
    let sent = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            producer
                .send(
                    FutureRecord::to(TOPIC).payload(&payload).key("push-e2e"),
                    Duration::from_secs(5),
                )
                .await
                .expect("produce event");

            let is_sent: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM notifications \
                 WHERE recipient = $1 AND channel::text = 'push' AND status = 'sent')",
            )
            .bind(&uid_str)
            .fetch_one(&pool)
            .await
            .expect("status query");

            if is_sent {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await;

    token.cancel();
    let _ = kafka_consumer.await;
    let _ = rabbit_consumer.await;

    sent.expect("push notification never reached `sent` within 120s — pipeline broken");

    // ---- 6. Terminal assertions -------------------------------------------
    // At least one push row reached `sent`, none in `permanent_failure`.
    let sent_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM notifications \
         WHERE recipient = $1 AND channel::text = 'push' AND status = 'sent'",
    )
    .bind(&uid_str)
    .fetch_one(&pool)
    .await
    .expect("sent-count query");
    assert!(sent_count >= 1, "no push row reached `sent`");

    let failed_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM notifications \
         WHERE recipient = $1 AND channel::text = 'push' AND status = 'permanent_failure'",
    )
    .bind(&uid_str)
    .fetch_one(&pool)
    .await
    .expect("failed-count query");
    assert_eq!(failed_count, 0, "a push notification ended in `permanent_failure`");

    // The FCM stub actually received a send for our seeded device token, with the
    // minted bearer attached (proves the fan-out + OAuth2 + per-platform envelope
    // legs ran on the wire, not just that a DB row flipped).
    let sends = stub_sends.lock().expect("stub sends lock");
    assert!(
        sends.iter().any(|s| s.body.contains(&device_token) && s.had_bearer),
        "FCM stub never received an authenticated send for the seeded device token"
    );
    assert!(
        sends.iter().any(|s| s.body.contains("\"android\"")),
        "send envelope missing the android platform block"
    );

    let _ = std::fs::remove_file(&sa_path);
}

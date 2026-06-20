//! Full-pipeline end-to-end test (closes gap #1).
//!
//! Every other integration test proves one leg in isolation: Kafka ingest,
//! `RabbitMQ` dispatch, the Postgres repo, the Redis cache. None proves the legs
//! are wired into a working whole. This boots the *four real brokers/stores* the
//! service depends on — Kafka, Postgres, Redis, `RabbitMQ` — assembles the
//! production dependency graph exactly as `startup::run` does (minus the HTTP /
//! gRPC servers), and drives a single event the whole way through:
//!
//!   produce `EmailVerified` to Kafka
//!     → `start_kafka_consumer` decodes + `orchestrator.queue_notification`
//!       → Redis idempotency check + Postgres insert (`pending`)
//!         → `RabbitMQ` `noti.dispatch` publish
//!           → `start_rabbitmq_consumer` → `orchestrator.dispatch`
//!             → Tera render (real `templates/`) → email provider send
//!               → Postgres `update_status(sent)`
//!
//! Terminal proof is the Postgres row reaching `sent` (only written *after* the
//! provider returns `Ok`, so it transitively proves render + delivery), plus the
//! capturing provider having received the rendered welcome email. A regression
//! anywhere along the chain — a missing binding, a wrong channel→provider map, a
//! template the producer's variables don't satisfy — fails here, where no
//! single-leg test can see it.
//!
//! Requires a Docker/OrbStack daemon — panics (does not skip) without one.
//! Slow by nature: four containers plus two consumer hops.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::duration_suboptimal_units
)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use noti_core::error::Result as NotiResult;
use noti_core::health::KafkaConsumerHealth;
use noti_core::traits::{
    CacheTrait, MessageQueueTrait, NotificationProviderTrait, NotificationRepositoryTrait,
    TemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::cache::CacheService;
use noti_persistence::messaging::rabbitmq::RabbitMQClient;
use noti_persistence::providers::{MockPushProvider, MockSmsProvider, MockWebSocketProvider, MockWebhookProvider};
use noti_persistence::repository::NotificationRepository;
use noti_persistence::templating::TemplateEngine;
use noti_server::consumers::{start_kafka_consumer, start_rabbitmq_consumer};

use testcontainers_modules::kafka::apache;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

const TOPIC: &str = "iam.user.events";

/// Email provider that records every `(recipient, content)` it is asked to send,
/// then succeeds — so the orchestrator marks the row `sent`, exactly like the
/// real `SmtpProvider` would on a successful delivery.
struct CapturingEmailProvider {
    sent: Arc<Mutex<Vec<(String, String)>>>,
}

#[async_trait]
impl NotificationProviderTrait for CapturingEmailProvider {
    async fn send(&self, recipient: &str, content: &str) -> NotiResult<String> {
        self.sent
            .lock()
            .expect("sent lock")
            .push((recipient.to_string(), content.to_string()));
        Ok(format!("captured-{}", Uuid::new_v4()))
    }
    fn provider_id(&self) -> &'static str {
        "capturing-email"
    }
}

async fn pg_url(c: &ContainerAsync<Postgres>) -> String {
    let host = c.get_host().await.expect("pg host");
    let port = c.get_host_port_ipv4(5432).await.expect("pg port");
    format!("postgres://postgres:postgres@{host}:{port}/postgres")
}

async fn redis_url(c: &ContainerAsync<Redis>) -> String {
    let host = c.get_host().await.expect("redis host");
    let port = c.get_host_port_ipv4(6379).await.expect("redis port");
    format!("redis://{host}:{port}")
}

async fn rabbit_url(c: &ContainerAsync<testcontainers_modules::rabbitmq::RabbitMq>) -> String {
    let host = c.get_host().await.expect("rabbit host");
    let port = c.get_host_port_ipv4(5672).await.expect("rabbit port");
    format!("amqp://{host}:{port}/%2f")
}

async fn kafka_brokers(c: &ContainerAsync<apache::Kafka>) -> String {
    let port = c
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("kafka port");
    format!("127.0.0.1:{port}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_flows_end_to_end_to_sent() {
    // ---- 1. Boot the four real dependencies ------------------------------
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

    let db_url = pg_url(&pg).await;
    let pool = PgPool::connect(&db_url).await.expect("connect pg pool");
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    // ---- 2. Assemble the production graph (mirrors startup::run) ----------
    let repo = NotificationRepository::new(pool.clone(), pool.clone());
    let cache = CacheService::new(&redis_url(&redis).await, "noti")
        .await
        .expect("connect cache");
    // `RabbitMQClient::new` declares the dispatch/retry topology the consumer
    // binds to, so it must run before the consumer starts.
    let mq_client = Arc::new(
        RabbitMQClient::new(&rabbit_url(&rabbit).await)
            .await
            .expect("connect rabbitmq"),
    );
    let templates = concat!(env!("CARGO_MANIFEST_DIR"), "/../../templates");
    let template_engine = TemplateEngine::new(templates).expect("load templates");

    let captured: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let email = Arc::new(CapturingEmailProvider {
        sent: captured.clone(),
    });

    let orchestrator = Arc::new(NotificationOrchestrator::new(
        Arc::new(repo) as Arc<dyn NotificationRepositoryTrait>,
        Arc::new(template_engine) as Arc<dyn TemplateEngineTrait>,
        email as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockSmsProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockPushProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockWebhookProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockWebSocketProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(cache) as Arc<dyn CacheTrait>,
        Some(mq_client.clone() as Arc<dyn MessageQueueTrait>),
    ));

    // ---- 3. Start the production consumers --------------------------------
    let token = CancellationToken::new();
    let health = Arc::new(KafkaConsumerHealth::new());

    let kafka_consumer = tokio::spawn(start_kafka_consumer(
        kafka_brokers(&kafka).await,
        format!("noti-e2e-{}", Uuid::new_v4()),
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

    // ---- 4. Produce a real event and wait for `sent` ----------------------
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers(&kafka).await)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("create producer");

    // Unique recipient so the run is self-contained and the status query is
    // unambiguous.
    let email_addr = format!("e2e-{}@example.com", Uuid::new_v4());
    let payload = serde_json::json!({
        "event_type": "EmailVerified",
        "data": { "email": email_addr, "username": "e2e-user" }
    })
    .to_string();

    // Produce repeatedly: the consumer group may still be rebalancing when the
    // first record lands. Each re-send lands at a new Kafka offset → a distinct
    // idempotency key, so a few duplicate rows for this recipient are possible;
    // we only assert at least one reached `sent`. Stop as soon as it does.
    let sent = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            producer
                .send(
                    FutureRecord::to(TOPIC).payload(&payload).key("e2e"),
                    Duration::from_secs(5),
                )
                .await
                .expect("produce event");

            let is_sent: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM notifications WHERE recipient = $1 AND status = 'sent')",
            )
            .bind(&email_addr)
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

    sent.expect("notification never reached `sent` within 120s — pipeline broken");

    // ---- 5. Terminal assertions -------------------------------------------
    // The capturing provider must have received the rendered welcome email for
    // our recipient (proves the channel→provider map + Tera render leg). Take a
    // copy and drop the guard before the awaits below.
    let delivered_content = {
        let deliveries = captured.lock().expect("captured lock");
        deliveries
            .iter()
            .find(|(to, _)| to == &email_addr)
            .map(|(_, content)| content.clone())
            .expect("email provider never received our recipient")
    };
    assert!(
        !delivered_content.trim().is_empty(),
        "delivered email content was empty (template render produced nothing)"
    );

    // And at least one persisted row is `sent` (written only after the provider
    // succeeded), with none landing in `permanent_failure`. Re-producing to beat
    // the consumer-group rebalance lands each record at a new Kafka offset → a
    // distinct idempotency key, so several rows for this recipient may exist;
    // the guarantee is that the pipeline drove the delivery to `sent`, not that
    // exactly one row was created.
    let sent_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM notifications WHERE recipient = $1 AND status = 'sent'",
    )
    .bind(&email_addr)
    .fetch_one(&pool)
    .await
    .expect("sent-count query");
    assert!(sent_count >= 1, "no notification row reached `sent`");

    let failed_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM notifications WHERE recipient = $1 AND status = 'permanent_failure'",
    )
    .bind(&email_addr)
    .fetch_one(&pool)
    .await
    .expect("failed-count query");
    assert_eq!(failed_count, 0, "a notification ended in `permanent_failure`");
}

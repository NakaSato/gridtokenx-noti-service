//! Retry-loop runtime test (closes gap #4).
//!
//! The pieces of the retry path are each tested in isolation: the `noti.retry`
//! DLX redelivery topology (`noti-persistence/tests/rabbitmq_dlx.rs`), the
//! ack/nack decision (`consumers::process_dispatch_payload`), and the
//! orchestrator's retry branching with mocked adapters (`noti-logic` dispatch
//! unit tests). None proves the runtime loop end to end: a real provider
//! failure driving the *real* `orchestrator.dispatch` to schedule a retry on a
//! *real* `RabbitMQ` queue with the notification's Postgres status flipped back
//! to `pending`, then a redelivery completing the loop to `sent` once the
//! provider recovers.
//!
//! Phase 1 (schedule leg): a flaky provider fails one delivery. The real
//! orchestrator must persist `pending` + `retry_count = 1` + the error, and its
//! real `publish_retry` must land a task on the live `noti.retry` queue.
//!
//! Phase 2 (completion leg): the provider recovers; we inject the redelivery
//! onto `noti.dispatch` (the broker's TTL-based DLX redelivery itself is proven
//! in `rabbitmq_dlx.rs`; reproducing the production 2-minute backoff here would
//! make the test minutes long for no extra coverage) and run the real
//! `start_rabbitmq_consumer`, which must re-dispatch through the orchestrator to
//! a terminal `sent`.
//!
//! Real Postgres + real `RabbitMQ`. Requires a Docker/OrbStack daemon — panics
//! (does not skip) without one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lapin::options::BasicGetOptions;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::{NotiError, Result};
use noti_core::traits::{
    CacheTrait, MessageQueueTrait, NotificationProviderTrait, NotificationRepositoryTrait,
    TemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::messaging::rabbitmq::RabbitMQClient;
use noti_persistence::repository::NotificationRepository;
use noti_server::consumers::start_rabbitmq_consumer;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;

/// Email provider whose deliveries fail while `fail` is set, and that counts
/// every attempt — so the test can flip it from failing to recovered and assert
/// the loop actually retried.
struct FlakyProvider {
    fail: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl NotificationProviderTrait for FlakyProvider {
    async fn send(&self, _recipient: &str, _content: &str) -> Result<String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(NotiError::Internal("simulated provider outage".into()))
        } else {
            Ok("delivered-ref".into())
        }
    }
    fn provider_id(&self) -> &'static str {
        "flaky-email"
    }
}

struct StubTemplate;
impl TemplateEngineTrait for StubTemplate {
    fn render(&self, _i: &str, _v: &serde_json::Value) -> Result<String> {
        Ok("body".into())
    }
}

struct StubCache;
#[async_trait]
impl CacheTrait for StubCache {
    async fn set_value(&self, _k: &str, _v: serde_json::Value, _t: u64) -> Result<()> {
        Ok(())
    }
    async fn get_value(&self, _k: &str) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }
    async fn increment_with_ttl(&self, _k: &str, _t: u64) -> Result<i64> {
        Ok(1)
    }
    async fn delete(&self, _k: &str) -> Result<()> {
        Ok(())
    }
    async fn lock(&self, _k: &str, _t: u64) -> Result<bool> {
        Ok(true)
    }
    async fn unlock(&self, _k: &str) -> Result<()> {
        Ok(())
    }
}

struct StubProvider;
#[async_trait]
impl NotificationProviderTrait for StubProvider {
    async fn send(&self, _r: &str, _b: &str) -> Result<String> {
        Ok("ref".into())
    }
    fn provider_id(&self) -> &'static str {
        "stub"
    }
}

fn pending_email(id: Uuid) -> Notification {
    let now = gridtokenx_telemetry::time::now();
    Notification {
        id,
        user_id: Some(Uuid::new_v4()),
        channel: NotificationChannel::Email,
        status: NotificationStatus::Pending,
        recipient: "retry@example.com".into(),
        template_id: "welcome".into(),
        variables: serde_json::json!({}),
        provider_id: None,
        provider_ref: None,
        retry_count: 0,
        next_retry_at: now,
        error_message: None,
        idempotency_key: None,
        created_at: now,
        updated_at: now,
        sent_at: None,
        read_at: None,
    }
}

async fn status_of(pool: &PgPool, id: Uuid) -> (String, i32) {
    sqlx::query_as("SELECT status::text, retry_count FROM notifications WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("status query")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_dispatch_schedules_retry_then_completes_on_redelivery() {
    // ---- Real Postgres + RabbitMQ ----------------------------------------
    let pg = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres");
    let rabbit = RabbitMq::default().start().await.expect("start rabbitmq");

    let pg_host = pg.get_host().await.expect("pg host");
    let pg_port = pg.get_host_port_ipv4(5432).await.expect("pg port");
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@{pg_host}:{pg_port}/postgres"
    ))
    .await
    .expect("connect pg");
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("migrate");

    let rb_host = rabbit.get_host().await.expect("rabbit host");
    let rb_port = rabbit.get_host_port_ipv4(5672).await.expect("rabbit port");
    let mq_client = Arc::new(
        RabbitMQClient::new(&format!("amqp://guest:guest@{rb_host}:{rb_port}"))
            .await
            .expect("connect rabbitmq"),
    );

    // ---- Orchestrator with a flaky email provider ------------------------
    let repo = Arc::new(NotificationRepository::new(pool.clone(), pool.clone()));
    let fail = Arc::new(AtomicBool::new(true));
    let calls = Arc::new(AtomicUsize::new(0));
    let email: Arc<dyn NotificationProviderTrait> = Arc::new(FlakyProvider {
        fail: fail.clone(),
        calls: calls.clone(),
    });
    let stub: Arc<dyn NotificationProviderTrait> = Arc::new(StubProvider);
    let orchestrator = Arc::new(NotificationOrchestrator::new(
        repo.clone() as Arc<dyn NotificationRepositoryTrait>,
        Arc::new(StubTemplate) as Arc<dyn TemplateEngineTrait>,
        email,
        stub.clone(),
        stub.clone(),
        stub.clone(),
        stub,
        Arc::new(StubCache) as Arc<dyn CacheTrait>,
        Some(mq_client.clone() as Arc<dyn MessageQueueTrait>),
    ));

    let id = Uuid::new_v4();
    repo.create(&pending_email(id))
        .await
        .expect("seed pending notification");

    // ---- Phase 1: dispatch fails → real retry scheduled ------------------
    orchestrator
        .clone()
        .dispatch(id)
        .await
        .expect("dispatch returns Ok (retry scheduled out-of-band)");

    assert_eq!(calls.load(Ordering::SeqCst), 1, "provider attempted once");
    let (status, retry_count) = status_of(&pool, id).await;
    assert_eq!(status, "pending", "row reset to pending for retry");
    assert_eq!(retry_count, 1, "retry_count incremented");

    // The orchestrator's real `publish_retry` must have landed a task on the
    // live `noti.retry` queue. Consume it (no_ack removes it so it can't later
    // dead-letter and interfere).
    let retry_msg = mq_client
        .channel()
        .basic_get("noti.retry", BasicGetOptions { no_ack: true })
        .await
        .expect("basic_get noti.retry");
    let retry_msg = retry_msg.expect("a retry task must be queued on noti.retry");
    let task: noti_persistence::messaging::rabbitmq::NotificationTask =
        serde_json::from_slice(&retry_msg.data).expect("decode retry task");
    assert_eq!(task.notification_id, id, "retry task targets our notification");

    // ---- Phase 2: provider recovers → redelivery completes to sent -------
    fail.store(false, Ordering::SeqCst);

    let token = CancellationToken::new();
    let consumer = tokio::spawn(start_rabbitmq_consumer(
        mq_client.clone(),
        orchestrator.clone(),
        token.clone(),
    ));

    // Inject the redelivery the DLX would perform after the backoff TTL.
    mq_client
        .publish_dispatch(id)
        .await
        .expect("re-enqueue dispatch task");

    let sent = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if status_of(&pool, id).await.0 == "sent" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;

    token.cancel();
    let _ = consumer.await;

    sent.expect("notification never reached `sent` after redelivery");
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "provider was retried after the first failure"
    );
    let (final_status, _) = status_of(&pool, id).await;
    assert_eq!(final_status, "sent", "loop completed to sent");
}

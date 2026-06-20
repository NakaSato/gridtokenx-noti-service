//! Boot-time crash-recovery end-to-end test (closes the `recover_pending` gap).
//!
//! `reset_stuck_processing` (the raw SQL that flips a crash-stranded `Processing`
//! row back to `Pending`) is exercised in `noti-persistence/tests/pg_repository.rs`.
//! `startup_smoke` boots `run()` but over an *empty* database, so the recovery
//! sweep runs across zero rows — the re-dispatch leg it guards
//! (`orchestrator.recover_pending`: reset stuck → fetch due `Pending` →
//! re-publish to `RabbitMQ` `noti.dispatch` → consumer → `dispatch` → `Sent`) was
//! never driven end to end. A regression that reset stuck rows but failed to
//! re-publish them — or published ids the consumer can't deliver — would leave
//! recovered notifications silently stuck, and no test would see it.
//!
//! This assembles the production graph `startup::run` builds (minus Kafka and the
//! HTTP/gRPC servers) against three real backends — Postgres, Redis, `RabbitMQ` —
//! seeds the exact state a mid-dispatch crash leaves behind — row A stranded in
//! `Processing` (a delivery the crashed process never finished) and row B left
//! `Pending` and past-due (a queued delivery never picked up) — then calls
//! `recover_pending` and proves *both* rows reach `Sent` and the email provider
//! actually received two deliveries.
//!
//! Requires a Docker/OrbStack daemon — panics (does not skip) without one.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::duration_suboptimal_units)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::Result as NotiResult;
use noti_core::traits::{
    CacheTrait, MessageQueueTrait, MockTemplateEngineTrait, NotificationProviderTrait,
    NotificationRepositoryTrait, TemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::cache::CacheService;
use noti_persistence::messaging::rabbitmq::RabbitMQClient;
use noti_persistence::providers::{
    MockPushProvider, MockSmsProvider, MockWebSocketProvider, MockWebhookProvider,
};
use noti_persistence::repository::NotificationRepository;
use noti_server::consumers::start_rabbitmq_consumer;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;

mod common;
use common::{pg_url, rabbit_url, redis_url};
use tokio_util::sync::CancellationToken;

/// Email provider that records every `(recipient, content)` it is asked to send,
/// then succeeds — so the orchestrator marks the row `sent`, exactly like the
/// real `SmtpProvider` on a successful delivery.
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


/// A minimal `Email` notification, `Pending`, due now.
fn sample(id: Uuid) -> Notification {
    let now = gridtokenx_telemetry::time::now();
    Notification {
        id,
        user_id: Some(Uuid::new_v4()),
        channel: NotificationChannel::Email,
        status: NotificationStatus::Pending,
        recipient: format!("recover-{id}@example.com"),
        template_id: "welcome.html.tera".to_string(),
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

/// Poll `get_by_id(id)` until its status is `Sent`, failing on timeout.
async fn wait_for_sent(repo: &NotificationRepository, id: Uuid) {
    let reached = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let row = repo.get_by_id(id).await.expect("get_by_id");
            if matches!(row.map(|n| n.status), Some(NotificationStatus::Sent)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "notification {id} never reached Sent within 60s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recover_pending_redispatches_crash_stranded_rows_to_sent() {
    // ---- 1. Boot the three real backends ---------------------------------
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

    let pool = PgPool::connect(&pg_url(&pg).await)
        .await
        .expect("connect pg pool");
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

    // Mock template (fixed body) so the test is independent of template content;
    // the recovery leg under test is the re-dispatch, not rendering.
    let mut template = MockTemplateEngineTrait::new();
    template
        .expect_render()
        .times(..)
        .returning(|_, _| Ok("recovered-body".to_string()));

    let captured: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let email = Arc::new(CapturingEmailProvider {
        sent: captured.clone(),
    });

    let orchestrator = Arc::new(NotificationOrchestrator::new(
        Arc::new(NotificationRepository::new(pool.clone(), pool.clone()))
            as Arc<dyn NotificationRepositoryTrait>,
        Arc::new(template) as Arc<dyn TemplateEngineTrait>,
        email as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockSmsProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockPushProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockWebhookProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(MockWebSocketProvider) as Arc<dyn NotificationProviderTrait>,
        Arc::new(cache) as Arc<dyn CacheTrait>,
        Some(mq_client.clone() as Arc<dyn MessageQueueTrait>),
    ));

    // ---- 3. Seed the exact state a mid-dispatch crash leaves behind -------
    // Row A: a delivery the crashed process started but never finished →
    // stranded in `Processing`.
    let stuck = Uuid::new_v4();
    repo.create(&sample(stuck)).await.expect("create stuck row");
    repo.update_status(stuck, NotificationStatus::Processing, None, None)
        .await
        .expect("strand row in Processing");

    // Row B: a queued delivery the crashed process never picked up → `Pending`,
    // due now (`next_retry_at` is creation time, already past by sweep time).
    let due = Uuid::new_v4();
    repo.create(&sample(due)).await.expect("create due row");

    // Sanity: the stuck row really is Processing before recovery runs.
    assert_eq!(
        repo.get_by_id(stuck).await.unwrap().unwrap().status,
        NotificationStatus::Processing,
        "row must start stranded in Processing"
    );

    // ---- 4. Start the production dispatch consumer ------------------------
    let token = CancellationToken::new();
    let consumer = tokio::spawn(start_rabbitmq_consumer(
        mq_client.clone(),
        orchestrator.clone(),
        token.clone(),
    ));

    // ---- 5. Run the boot recovery sweep -----------------------------------
    // threshold 0 → any Processing row is "stuck"; batch 100 covers both rows.
    let requeued = orchestrator
        .recover_pending(0, 100)
        .await
        .expect("recover_pending");
    assert_eq!(requeued, 2, "both the reset-stuck and the due row must be re-queued");

    // ---- 6. Both rows must travel the full re-dispatch path to Sent -------
    wait_for_sent(&repo, stuck).await;
    wait_for_sent(&repo, due).await;

    // The email provider actually received both recovered deliveries.
    let delivered = captured.lock().expect("sent lock").len();
    assert_eq!(delivered, 2, "email provider must have sent both recovered rows");

    // ---- 7. Graceful shutdown --------------------------------------------
    token.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(5), consumer).await;
    assert!(
        matches!(stopped, Ok(Ok(Ok(())))),
        "consumer did not stop gracefully on cancel: {stopped:?}"
    );
}

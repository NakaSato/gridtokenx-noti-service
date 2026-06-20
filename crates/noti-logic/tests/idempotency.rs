//! Unit tests for `queue_notification`'s idempotency dedup behavior.
//!
//! `pg_repository` proves the *repository's* `ON CONFLICT (idempotency_key)`
//! keeps a single row; `full_pipeline` explicitly punts on dedup (it asserts
//! only that ≥1 row reaches `sent` and tolerates duplicate rows from re-sends).
//! Nothing pinned the *orchestrator* contract the dedup actually rests on:
//! `queue_notification` builds a row with a fresh id, and `repo.create` echoes
//! that id only on a genuine insert; a conflict returns the *existing* row's id.
//! The orchestrator keys off `returned_id != requested_id` (`queue.rs:56`) to
//! short-circuit — skipping both the Redis key cache and the dispatch publish so
//! a duplicate event is never delivered twice.
//!
//! A regression that published before that check, or compared the wrong ids,
//! would double-send every retried/at-least-once event with no test to catch it.
//! These mock every adapter (mockall) and assert publish/cache call *counts*.

use std::sync::Arc;

use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::traits::{
    MessageQueueTrait, MockCacheTrait, MockMessageQueueTrait, MockNotificationProviderTrait,
    MockNotificationRepositoryTrait, MockTemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;

/// Assemble an orchestrator from a pre-configured repo, cache, and MQ. Providers
/// and template are bare mocks — `queue_notification` touches none of them.
fn orchestrator(
    repo: MockNotificationRepositoryTrait,
    cache: MockCacheTrait,
    mq: MockMessageQueueTrait,
) -> Arc<NotificationOrchestrator> {
    Arc::new(NotificationOrchestrator::new(
        Arc::new(repo),
        Arc::new(MockTemplateEngineTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()), // email
        Arc::new(MockNotificationProviderTrait::new()), // sms
        Arc::new(MockNotificationProviderTrait::new()), // push
        Arc::new(MockNotificationProviderTrait::new()), // webhook
        Arc::new(MockNotificationProviderTrait::new()), // websocket
        Arc::new(cache),
        Some(Arc::new(mq) as Arc<dyn MessageQueueTrait>),
    ))
}

/// An already-persisted row carrying a *fixed* id — what `repo.create` returns
/// when the incoming `idempotency_key` conflicts with an existing notification.
fn existing_row(id: Uuid) -> Notification {
    let now = gridtokenx_telemetry::time::now();
    Notification {
        id,
        user_id: None,
        channel: NotificationChannel::Email,
        status: NotificationStatus::Pending,
        recipient: "user@example.com".to_string(),
        template_id: "welcome.html.tera".to_string(),
        variables: serde_json::json!({}),
        provider_id: None,
        provider_ref: None,
        retry_count: 0,
        next_retry_at: now,
        error_message: None,
        idempotency_key: Some("dup-key".to_string()),
        created_at: now,
        updated_at: now,
        sent_at: None,
        read_at: None,
    }
}

/// Fresh key → genuine insert (`create` echoes the row's id) → orchestrator
/// caches the idempotency key and publishes exactly one dispatch task.
#[tokio::test]
async fn fresh_idempotency_key_caches_and_publishes_once() {
    let mut repo = MockNotificationRepositoryTrait::new();
    // A genuine insert: echo the row back unchanged → returned id == requested id.
    repo.expect_create().times(1).returning(|n| Ok(n.clone()));

    let mut cache = MockCacheTrait::new();
    cache
        .expect_set_value()
        .times(1)
        .returning(|_, _, _| Ok(()));

    let mut mq = MockMessageQueueTrait::new();
    mq.expect_publish_dispatch().times(1).returning(|_| Ok(()));

    let orch = orchestrator(repo, cache, mq);
    let id = orch
        .queue_notification(
            None,
            NotificationChannel::Email,
            "user@example.com".to_string(),
            "welcome.html.tera".to_string(),
            serde_json::json!({}),
            Some("fresh-key".to_string()),
        )
        .await
        .expect("queue ok");

    // The returned id is the freshly-minted one (non-nil). Call-count assertions
    // (set_value ×1, publish_dispatch ×1) are enforced by mockall on drop.
    assert!(!id.is_nil());
}

/// Duplicate key → `create` returns the *existing* row's id → orchestrator
/// short-circuits: NO cache write, NO dispatch publish, returns the existing id.
#[tokio::test]
async fn duplicate_idempotency_key_short_circuits_without_publishing() {
    let existing_id = Uuid::new_v4();
    let row = existing_row(existing_id);

    let mut repo = MockNotificationRepositoryTrait::new();
    // Conflict: the persisted (existing) row comes back with a *different* id
    // than the one `queue_notification` minted internally.
    repo.expect_create()
        .times(1)
        .returning(move |_| Ok(row.clone()));

    // The dedup branch must touch neither Redis nor the queue.
    let mut cache = MockCacheTrait::new();
    cache.expect_set_value().times(0);

    let mut mq = MockMessageQueueTrait::new();
    mq.expect_publish_dispatch().times(0);

    let orch = orchestrator(repo, cache, mq);
    let id = orch
        .queue_notification(
            None,
            NotificationChannel::Email,
            "user@example.com".to_string(),
            "welcome.html.tera".to_string(),
            serde_json::json!({}),
            Some("dup-key".to_string()),
        )
        .await
        .expect("queue ok");

    assert_eq!(
        id, existing_id,
        "duplicate must resolve to the existing row's id, not a new one"
    );
}

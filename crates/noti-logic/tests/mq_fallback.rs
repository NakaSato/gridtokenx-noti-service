//! Unit tests for `trigger_dispatch`'s in-process fallback (`queue.rs:81`).
//!
//! `queue_notification` normally hands delivery to `RabbitMQ`. Two degraded paths
//! existed with no test: MQ not configured at all (`mq: None` — dev/minimal
//! deployments) and MQ configured but the publish failing (broker down at
//! publish time). Both must fall back to an in-process `tokio::spawn` of
//! `dispatch`, so the notification still reaches its provider instead of
//! sitting in `pending` forever. A regression that dropped the fallback would
//! silently strand every notification queued while the broker was unavailable.
//!
//! The fallback dispatch runs on a spawned task, so each test signals
//! completion through a channel fired by the final `update_status(.., Sent)`.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use noti_core::domain::{NotificationChannel, NotificationStatus};
use noti_core::error::NotiError;
use noti_core::traits::{
    MessageQueueTrait, MockCacheTrait, MockMessageQueueTrait, MockNotificationProviderTrait,
    MockNotificationRepositoryTrait,
};
use noti_logic::NotificationOrchestrator;

mod common;

/// Repo whose `get_by_id` serves a `Pending` row for any id and whose
/// `update_status` reports every status transition to `tx` — the test awaits
/// the `Sent` transition to prove the spawned fallback dispatch completed.
fn reporting_repo(
    tx: tokio::sync::mpsc::UnboundedSender<NotificationStatus>,
) -> MockNotificationRepositoryTrait {
    let mut repo = MockNotificationRepositoryTrait::new();
    repo.expect_create().times(1).returning(|n| Ok(n.clone()));
    repo.expect_get_by_id().times(1).returning(|id| {
        Ok(Some(common::notification(
            id,
            NotificationChannel::Email,
            NotificationStatus::Pending,
            0,
        )))
    });
    repo.expect_update_status()
        .times(2) // Processing, then Sent
        .returning(move |_, status, _, _| {
            let _ = tx.send(status);
            Ok(())
        });
    repo
}

/// Email provider that succeeds; the other four channels get bare mocks.
fn orchestrator(
    repo: MockNotificationRepositoryTrait,
    mq: Option<Arc<dyn MessageQueueTrait>>,
) -> Arc<NotificationOrchestrator> {
    let mut email = MockNotificationProviderTrait::new();
    email
        .expect_send()
        .times(1)
        .returning(|_, _| Ok("provider-ref".to_string()));
    email.expect_provider_id().times(..).return_const("mock-email");

    Arc::new(NotificationOrchestrator::new(
        Arc::new(repo),
        Arc::new(common::ok_template()),
        Arc::new(email),
        Arc::new(MockNotificationProviderTrait::new()), // sms
        Arc::new(MockNotificationProviderTrait::new()), // push
        Arc::new(MockNotificationProviderTrait::new()), // webhook
        Arc::new(MockNotificationProviderTrait::new()), // websocket
        Arc::new(MockCacheTrait::new()), // no idempotency key → never touched
        mq,
    ))
}

/// Queue with no idempotency key, then await the spawned fallback dispatch
/// driving the row Processing → Sent.
async fn queue_and_await_sent(orch: Arc<NotificationOrchestrator>) -> Uuid {
    let id = orch
        .queue_notification(
            None,
            NotificationChannel::Email,
            "user@example.com".to_string(),
            "welcome.html.tera".to_string(),
            serde_json::json!({}),
            None,
        )
        .await
        .expect("queue ok");
    assert!(!id.is_nil());
    id
}

async fn await_sent(rx: &mut tokio::sync::mpsc::UnboundedReceiver<NotificationStatus>) {
    let sent = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(status) = rx.recv().await {
            if status == NotificationStatus::Sent {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(sent, Ok(true), "fallback dispatch never marked the row Sent");
}

/// No MQ configured → `trigger_dispatch` must spawn an in-process dispatch
/// that carries the notification all the way to `Sent`.
#[tokio::test]
async fn mq_absent_dispatches_inline_to_sent() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let orch = orchestrator(reporting_repo(tx), None);

    queue_and_await_sent(orch).await;
    await_sent(&mut rx).await;
}

/// MQ configured but the publish fails → same in-process fallback, so a broker
/// outage at publish time degrades to inline delivery instead of a lost task.
#[tokio::test]
async fn mq_publish_failure_falls_back_to_inline_dispatch() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let mut mq = MockMessageQueueTrait::new();
    mq.expect_publish_dispatch()
        .times(1)
        .returning(|_| Err(NotiError::Internal("broker down".to_string())));

    let orch = orchestrator(
        reporting_repo(tx),
        Some(Arc::new(mq) as Arc<dyn MessageQueueTrait>),
    );

    queue_and_await_sent(orch).await;
    await_sent(&mut rx).await;
}

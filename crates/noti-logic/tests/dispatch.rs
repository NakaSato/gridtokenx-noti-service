//! Unit tests for the dispatch + retry path of `NotificationOrchestrator`.
//!
//! Every adapter is mocked (mockall). These exercise the branching in
//! `orchestrator::dispatch`: success → `Sent`, transient failure → retry
//! scheduled, exhausted retries → `PermanentFailure`, template failure →
//! `PermanentFailure`, terminal status → no-op, and missing record → error.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::NotiError;
use noti_core::traits::{
    MockCacheTrait, MockMessageQueueTrait, MockNotificationProviderTrait,
    MockNotificationRepositoryTrait, MockTemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;

/// Build an Email-channel notification with the given status and retry count.
fn email_notification(id: Uuid, status: NotificationStatus, retry_count: i32) -> Notification {
    Notification {
        id,
        user_id: None,
        channel: NotificationChannel::Email,
        status,
        recipient: "user@example.com".to_string(),
        template_id: "welcome.html.tera".to_string(),
        variables: serde_json::json!({}),
        provider_id: None,
        provider_ref: None,
        retry_count,
        next_retry_at: Utc::now(),
        error_message: None,
        idempotency_key: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        sent_at: None,
        read_at: None,
    }
}

/// Template mock that always renders a fixed body.
fn ok_template() -> MockTemplateEngineTrait {
    let mut tmpl = MockTemplateEngineTrait::new();
    tmpl.expect_render()
        .times(..)
        .returning(|_, _| Ok("body".to_string()));
    tmpl
}

/// Assemble an orchestrator with the given email provider, repo, template, and
/// optional MQ. Other channel providers are bare mocks (panic if ever called).
fn orchestrator(
    repo: MockNotificationRepositoryTrait,
    tmpl: MockTemplateEngineTrait,
    email_provider: MockNotificationProviderTrait,
    mq: Option<MockMessageQueueTrait>,
) -> Arc<NotificationOrchestrator> {
    Arc::new(NotificationOrchestrator::new(
        Arc::new(repo),
        Arc::new(tmpl),
        Arc::new(email_provider),
        Arc::new(MockNotificationProviderTrait::new()), // sms
        Arc::new(MockNotificationProviderTrait::new()), // push
        Arc::new(MockNotificationProviderTrait::new()), // webhook
        Arc::new(MockNotificationProviderTrait::new()), // websocket
        Arc::new(MockCacheTrait::new()),
        mq.map(|m| Arc::new(m) as Arc<dyn noti_core::traits::MessageQueueTrait>),
    ))
}

/// A repo whose `update_status` records every status it is asked to write.
fn recording_repo(
    notif: Notification,
) -> (MockNotificationRepositoryTrait, Arc<Mutex<Vec<NotificationStatus>>>) {
    let statuses = Arc::new(Mutex::new(Vec::<NotificationStatus>::new()));
    let sink = statuses.clone();

    let mut repo = MockNotificationRepositoryTrait::new();
    repo.expect_get_by_id()
        .times(..)
        .returning(move |_| Ok(Some(notif.clone())));
    repo.expect_update_status()
        .times(..)
        .returning(move |_, status, _, _| {
            sink.lock().expect("status sink lock").push(status);
            Ok(())
        });
    (repo, statuses)
}

#[tokio::test]
async fn dispatch_success_marks_sent() {
    let id = Uuid::new_v4();
    let (repo, statuses) = recording_repo(email_notification(id, NotificationStatus::Pending, 0));

    let mut provider = MockNotificationProviderTrait::new();
    provider
        .expect_send()
        .times(1)
        .returning(|_, _| Ok("provider-ref-123".to_string()));
    provider.expect_provider_id().times(..).returning(|| "mock");

    let orch = orchestrator(repo, ok_template(), provider, None);
    Arc::clone(&orch).dispatch(id).await.expect("dispatch ok");

    let recorded = statuses.lock().expect("lock");
    assert!(
        recorded.contains(&NotificationStatus::Sent),
        "expected a Sent status write, got {recorded:?}"
    );
}

#[tokio::test]
async fn dispatch_transient_failure_schedules_retry() {
    let id = Uuid::new_v4();
    let (mut repo, statuses) =
        recording_repo(email_notification(id, NotificationStatus::Pending, 0));
    repo.expect_increment_retry().times(1).returning(|_, _| Ok(()));

    let mut provider = MockNotificationProviderTrait::new();
    provider
        .expect_send()
        .times(1)
        .returning(|_, _| Err(NotiError::Internal("smtp down".to_string())));

    let retries = Arc::new(Mutex::new(Vec::<(Uuid, u32)>::new()));
    let sink = retries.clone();
    let mut mq = MockMessageQueueTrait::new();
    mq.expect_publish_retry().times(1).returning(move |id, delay| {
        sink.lock().expect("retry sink").push((id, delay));
        Ok(())
    });

    let orch = orchestrator(repo, ok_template(), provider, Some(mq));
    // Transient failure must NOT bubble up — retry is scheduled, dispatch is Ok.
    Arc::clone(&orch).dispatch(id).await.expect("dispatch ok (retry scheduled)");

    let scheduled = retries.lock().expect("lock");
    assert_eq!(scheduled.len(), 1, "exactly one retry should be published");
    assert_eq!(scheduled[0].0, id);
    // retry_count 0 → 1 → delay = 2^1 * 60_000 = 120_000 ms.
    assert_eq!(scheduled[0].1, 120_000, "exponential backoff delay");

    let recorded = statuses.lock().expect("lock");
    assert!(
        recorded.contains(&NotificationStatus::Pending),
        "retry path must reset status to Pending, got {recorded:?}"
    );
}

#[tokio::test]
async fn dispatch_exhausted_retries_marks_permanent_failure() {
    let id = Uuid::new_v4();
    // retry_count == MAX_RETRIES (5) → no further retry, permanent failure.
    let (repo, statuses) = recording_repo(email_notification(id, NotificationStatus::Pending, 5));

    let mut provider = MockNotificationProviderTrait::new();
    provider
        .expect_send()
        .times(1)
        .returning(|_, _| Err(NotiError::Internal("smtp down".to_string())));

    // No MQ expectation: publish_retry must NOT be called on the exhausted path.
    let orch = orchestrator(repo, ok_template(), provider, None);
    let result = Arc::clone(&orch).dispatch(id).await;

    assert!(result.is_err(), "exhausted retries should return the error");
    let recorded = statuses.lock().expect("lock");
    assert!(
        recorded.contains(&NotificationStatus::PermanentFailure),
        "expected PermanentFailure, got {recorded:?}"
    );
}

#[tokio::test]
async fn dispatch_template_failure_marks_permanent_failure() {
    let id = Uuid::new_v4();
    let (repo, statuses) = recording_repo(email_notification(id, NotificationStatus::Pending, 0));

    let mut tmpl = MockTemplateEngineTrait::new();
    tmpl.expect_render()
        .times(1)
        .returning(|_, _| Err(NotiError::Internal("missing template".to_string())));

    // Provider has no expectation: a template failure must short-circuit before send.
    let orch = orchestrator(repo, tmpl, MockNotificationProviderTrait::new(), None);
    let result = Arc::clone(&orch).dispatch(id).await;

    assert!(result.is_err(), "template failure should return the error");
    let recorded = statuses.lock().expect("lock");
    assert!(
        recorded.contains(&NotificationStatus::PermanentFailure),
        "expected PermanentFailure, got {recorded:?}"
    );
}

#[tokio::test]
async fn dispatch_skips_terminal_status() {
    let id = Uuid::new_v4();
    let notif = email_notification(id, NotificationStatus::Sent, 0);

    let mut repo = MockNotificationRepositoryTrait::new();
    repo.expect_get_by_id()
        .times(1)
        .returning(move |_| Ok(Some(notif.clone())));
    // No update_status / provider expectations: an already-Sent record is a no-op.

    let orch = orchestrator(repo, ok_template(), MockNotificationProviderTrait::new(), None);
    Arc::clone(&orch)
        .dispatch(id)
        .await
        .expect("terminal status is a no-op");
}

#[tokio::test]
async fn dispatch_missing_notification_errors() {
    let id = Uuid::new_v4();
    let mut repo = MockNotificationRepositoryTrait::new();
    repo.expect_get_by_id().times(1).returning(|_| Ok(None));

    let orch = orchestrator(repo, ok_template(), MockNotificationProviderTrait::new(), None);
    let result = Arc::clone(&orch).dispatch(id).await;

    assert!(
        matches!(result, Err(NotiError::NotFound(_))),
        "missing notification should be NotFound, got {result:?}"
    );
}

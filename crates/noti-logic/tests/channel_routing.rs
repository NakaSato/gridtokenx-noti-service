//! Channel→provider routing tests for `orchestrator::dispatch` (closes gap #4).
//!
//! `dispatch.rs` exercises the success/retry/failure *branching*, but only ever
//! over the Email channel — the other four provider slots are bare mocks that
//! are never reached. Nothing pins the `match notification.channel { ... }`
//! provider-selection arms: a regression that swapped two (e.g. routed `Push`
//! to the SMS provider, or `WebSocket` to Webhook) would deliver silently to the
//! wrong transport and every existing test would still pass.
//!
//! This drives one notification per channel through the real orchestrator with
//! all five providers wired as distinct mocks. The provider for the channel
//! under test returns a **channel-unique** provider ref; the other four are set
//! to `times(0)` so being called at all fails the test. The assertion is that
//! dispatch persisted exactly that channel's unique ref — proving the right
//! provider, and only it, was selected.

use std::sync::{Arc, Mutex};

use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::traits::{
    MessageQueueTrait, MockCacheTrait, MockNotificationProviderTrait,
    MockNotificationRepositoryTrait,
};
use noti_logic::NotificationOrchestrator;

mod common;
use common::ok_template;

/// Provider ref a given channel's provider returns when (correctly) selected.
fn expected_ref(channel: &NotificationChannel) -> &'static str {
    match channel {
        NotificationChannel::Email => "ref-email",
        NotificationChannel::Sms => "ref-sms",
        NotificationChannel::Push => "ref-push",
        NotificationChannel::Webhook => "ref-webhook",
        NotificationChannel::WebSocket => "ref-websocket",
    }
}

/// A provider that must be selected: returns `reference` on its single send.
fn selected_provider(reference: &'static str) -> Arc<dyn noti_core::traits::NotificationProviderTrait> {
    let mut p = MockNotificationProviderTrait::new();
    p.expect_send()
        .times(1)
        .returning(move |_, _| Ok(reference.to_string()));
    p.expect_provider_id().times(..).returning(|| "selected");
    Arc::new(p)
}

/// A provider that must never be selected: any `send` call fails the test.
fn forbidden_provider() -> Arc<dyn noti_core::traits::NotificationProviderTrait> {
    let mut p = MockNotificationProviderTrait::new();
    p.expect_send().times(0);
    Arc::new(p)
}

/// Repo that serves `notif` and records every `(status, provider_ref)` written.
type Writes = Arc<Mutex<Vec<(NotificationStatus, Option<String>)>>>;
fn recording_repo(notif: Notification) -> (MockNotificationRepositoryTrait, Writes) {
    let writes: Writes = Arc::new(Mutex::new(Vec::new()));
    let sink = writes.clone();

    let mut repo = MockNotificationRepositoryTrait::new();
    repo.expect_get_by_id()
        .times(..)
        .returning(move |_| Ok(Some(notif.clone())));
    repo.expect_update_status()
        .times(..)
        .returning(move |_, status, _, provider_ref| {
            sink.lock().expect("writes lock").push((status, provider_ref));
            Ok(())
        });
    (repo, writes)
}

/// Build an orchestrator placing `real` in the slot for `channel` and a
/// forbidden mock in every other slot.
fn orchestrator_routing(
    channel: &NotificationChannel,
    repo: MockNotificationRepositoryTrait,
    real: &Arc<dyn noti_core::traits::NotificationProviderTrait>,
) -> Arc<NotificationOrchestrator> {
    use std::mem::discriminant;
    use NotificationChannel::{Email, Push, Sms, WebSocket, Webhook};
    let want = discriminant(channel);
    let pick = |c: NotificationChannel| {
        if discriminant(&c) == want {
            real.clone()
        } else {
            forbidden_provider()
        }
    };
    let mq: Option<Arc<dyn MessageQueueTrait>> = None;
    Arc::new(NotificationOrchestrator::new(
        Arc::new(repo),
        Arc::new(ok_template()),
        pick(Email),
        pick(Sms),
        pick(Push),
        pick(Webhook),
        pick(WebSocket),
        Arc::new(MockCacheTrait::new()),
        mq,
    ))
}

/// Dispatch one notification of `channel`; assert it persisted exactly that
/// channel's unique provider ref (and only the right provider was called — the
/// others are `times(0)` and would panic if selected).
async fn assert_routes(channel: NotificationChannel) {
    let id = Uuid::new_v4();
    let reference = expected_ref(&channel);
    let (repo, writes) = recording_repo(common::notification(
        id,
        channel.clone(),
        NotificationStatus::Pending,
        0,
    ));
    let real = selected_provider(reference);
    let orch = orchestrator_routing(&channel, repo, &real);

    Arc::clone(&orch)
        .dispatch(id)
        .await
        .unwrap_or_else(|e| panic!("dispatch for {channel:?} failed: {e}"));

    let recorded = writes.lock().expect("writes lock");
    assert!(
        recorded
            .iter()
            .any(|(s, r)| *s == NotificationStatus::Sent && r.as_deref() == Some(reference)),
        "{channel:?} must persist Sent with its own provider ref {reference:?}, got {recorded:?}"
    );
}

#[tokio::test]
async fn email_routes_to_email_provider() {
    assert_routes(NotificationChannel::Email).await;
}

#[tokio::test]
async fn sms_routes_to_sms_provider() {
    assert_routes(NotificationChannel::Sms).await;
}

#[tokio::test]
async fn push_routes_to_push_provider() {
    assert_routes(NotificationChannel::Push).await;
}

#[tokio::test]
async fn webhook_routes_to_webhook_provider() {
    assert_routes(NotificationChannel::Webhook).await;
}

#[tokio::test]
async fn websocket_routes_to_websocket_provider() {
    assert_routes(NotificationChannel::WebSocket).await;
}

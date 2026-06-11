//! Integration test: a `Webhook`-channel notification dispatched through the
//! real `NotificationOrchestrator` reaches the real `WebhookProvider`, which
//! performs an actual HTTP POST to the target. Repo/cache/template/MQ are
//! mocked (mockall); only the webhook delivery path is real.
//!
//! The SSRF guard normally blocks loopback targets, so the provider is built
//! with `with_block_private(false)` to allow the local stub server.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::traits::{
    MockCacheTrait, MockNotificationProviderTrait, MockNotificationRepositoryTrait,
    MockTemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::providers::webhook::WebhookProvider;

/// Minimal one-shot HTTP/1.1 stub: accepts a single connection, captures the
/// raw request, replies `200 OK`, and forwards the request text over a channel.
async fn spawn_stub() -> (String, tokio::sync::oneshot::Receiver<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub server");
    let port = listener.local_addr().expect("local addr").port();
    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = vec![0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).into_owned();
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await;
            let _ = sock.flush().await;
            let _ = tx.send(req);
        }
    });

    (format!("http://127.0.0.1:{port}/hook"), rx)
}

fn webhook_notification(id: Uuid, recipient: String) -> Notification {
    Notification {
        id,
        user_id: None,
        channel: NotificationChannel::Webhook,
        status: NotificationStatus::Pending,
        recipient,
        template_id: "webhook.txt.tera".to_string(),
        variables: serde_json::json!({}),
        provider_id: None,
        provider_ref: None,
        retry_count: 0,
        next_retry_at: Utc::now(),
        error_message: None,
        idempotency_key: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        sent_at: None,
        read_at: None,
    }
}

#[tokio::test]
async fn webhook_notification_dispatches_real_http_post() {
    let (url, rx) = spawn_stub().await;

    let id = Uuid::new_v4();
    let notif = webhook_notification(id, url);

    // Repo: return the pending notification on lookup; accept status updates.
    let mut repo = MockNotificationRepositoryTrait::new();
    repo.expect_get_by_id()
        .times(..)
        .returning(move |_| Ok(Some(notif.clone())));
    repo.expect_update_status().times(..).returning(|_, _, _, _| Ok(()));

    // Template: render to a fixed body we can assert on the wire.
    let mut tmpl = MockTemplateEngineTrait::new();
    tmpl.expect_render()
        .times(..)
        .returning(|_, _| Ok("payload body".to_string()));

    let cache = MockCacheTrait::new();

    let orchestrator = Arc::new(NotificationOrchestrator::new(
        Arc::new(repo),
        Arc::new(tmpl),
        Arc::new(MockNotificationProviderTrait::new()), // email (unused)
        Arc::new(MockNotificationProviderTrait::new()), // sms   (unused)
        Arc::new(MockNotificationProviderTrait::new()), // push  (unused)
        Arc::new(WebhookProvider::with_block_private(false)), // webhook (REAL)
        Arc::new(MockNotificationProviderTrait::new()), // websocket (unused)
        Arc::new(cache),
        None, // no MQ — dispatch runs synchronously here
    ));

    let result = Arc::clone(&orchestrator).dispatch(id).await;
    assert!(result.is_ok(), "dispatch failed: {result:?}");

    let req = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("stub server did not receive a request in time")
        .expect("stub server dropped the request");

    assert!(req.starts_with("POST "), "expected POST request, got: {req}");
    assert!(
        req.contains("payload body"),
        "rendered body missing from request: {req}"
    );
}

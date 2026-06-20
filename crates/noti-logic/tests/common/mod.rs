//! Shared leaf helpers for the `noti-logic` orchestrator tests.
//!
//! Each test file builds `Notification` fixtures and an always-rendering
//! template mock. Those were duplicated; the canonical versions live here.
//! Test-specific assemblers (recording repos, the per-file orchestrator wiring)
//! stay in their own files since they capture different calls.
//!
//! Compiled once per test binary, so not every item is used by every file.

#![allow(dead_code)]

use chrono::Utc;
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::traits::MockTemplateEngineTrait;

/// A notification with the given channel/status/retry-count and otherwise fixed,
/// inert fields. Covers every fixture the orchestrator tests need.
pub fn notification(
    id: Uuid,
    channel: NotificationChannel,
    status: NotificationStatus,
    retry_count: i32,
) -> Notification {
    let now = Utc::now();
    Notification {
        id,
        user_id: None,
        channel,
        status,
        recipient: "user@example.com".to_string(),
        template_id: "welcome.html.tera".to_string(),
        variables: serde_json::json!({}),
        provider_id: None,
        provider_ref: None,
        retry_count,
        next_retry_at: now,
        error_message: None,
        idempotency_key: None,
        created_at: now,
        updated_at: now,
        sent_at: None,
        read_at: None,
    }
}

/// Template mock that renders a fixed body for any input.
pub fn ok_template() -> MockTemplateEngineTrait {
    let mut tmpl = MockTemplateEngineTrait::new();
    tmpl.expect_render()
        .times(..)
        .returning(|_, _| Ok("body".to_string()));
    tmpl
}

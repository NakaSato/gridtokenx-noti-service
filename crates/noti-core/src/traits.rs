//! Trait contracts consumed by the logic layer.
//!
//! All traits use `async_trait` and return `noti_core::error::Result`
//! to enforce a uniform error surface across adapters.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::{Notification, NotificationStatus};
use crate::error::Result;

// ---------------------------------------------------------------------------
// Repository
// ---------------------------------------------------------------------------

/// Persistent storage for notification records.
#[cfg_attr(feature = "mocks", mockall::automock)]
#[async_trait]
pub trait NotificationRepositoryTrait: Send + Sync {
    async fn create(&self, notification: &Notification) -> Result<Notification>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Notification>>;
    async fn get_by_idempotency_key(&self, key: &str) -> Result<Option<Notification>>;
    async fn update_status(
        &self,
        id: Uuid,
        status: NotificationStatus,
        error: Option<String>,
        provider_ref: Option<String>,
    ) -> Result<()>;
    async fn increment_retry(&self, id: Uuid, next_retry_at: DateTime<Utc>) -> Result<()>;
    async fn get_pending_for_retry(&self, limit: i32) -> Result<Vec<Notification>>;
    /// Reset rows stuck in `Processing` for longer than `stuck_threshold_secs`
    /// (e.g. a crash mid-dispatch) back to `Pending`, due immediately, so a
    /// boot-time sweep re-dispatches them. Returns the number of rows reset.
    async fn reset_stuck_processing(&self, stuck_threshold_secs: i64) -> Result<u64>;

    // User-facing operations
    async fn list_by_user(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Notification>>;
    async fn mark_as_read(&self, id: Uuid, user_id: Uuid) -> Result<()>;
    async fn mark_all_as_read(&self, user_id: Uuid) -> Result<()>;
    async fn get_unread_count(&self, user_id: Uuid) -> Result<i64>;
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

/// External delivery provider (Email, SMS, Push, Webhook, WebSocket).
#[cfg_attr(feature = "mocks", mockall::automock)]
#[async_trait]
pub trait NotificationProviderTrait: Send + Sync {
    /// Deliver content to the given recipient. Returns a provider reference ID.
    async fn send(&self, recipient: &str, content: &str) -> Result<String>;
    /// Human-readable provider identifier for logging / audit.
    fn provider_id(&self) -> &'static str;
}

/// Registry for active WebSocket connections.
#[cfg_attr(feature = "mocks", mockall::automock)]
#[async_trait]
pub trait WebSocketRegistryTrait: Send + Sync {
    /// Send a message to a specific user via WebSocket.
    async fn send_to_user(&self, user_id: &uuid::Uuid, message: &str) -> Result<bool>;
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

/// Transient key-value cache (Redis-backed in production).
#[cfg_attr(feature = "mocks", mockall::automock)]
#[async_trait]
pub trait CacheTrait: Send + Sync {
    async fn set_value(&self, key: &str, value: serde_json::Value, ttl_secs: u64) -> Result<()>;
    async fn get_value(&self, key: &str) -> Result<Option<serde_json::Value>>;
    async fn increment_with_ttl(&self, key: &str, ttl_secs: u64) -> Result<i64>;
    async fn delete(&self, key: &str) -> Result<()>;
    /// Acquires a distributed lock.
    async fn lock(&self, key: &str, ttl_secs: u64) -> Result<bool>;
    /// Releases a distributed lock.
    async fn unlock(&self, key: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Template Engine
// ---------------------------------------------------------------------------

/// Template rendering (sync — pure string transformation).
///
/// # Errors
///
/// Returns `NotiError` if the template is not found or rendering fails.
#[cfg_attr(feature = "mocks", mockall::automock)]
pub trait TemplateEngineTrait: Send + Sync {
    /// # Errors
    ///
    /// Returns `NotiError` if the template is not found or rendering fails.
    fn render(&self, template_id: &str, variables: &serde_json::Value) -> Result<String>;
}

// ---------------------------------------------------------------------------
// Message Queue
// ---------------------------------------------------------------------------

/// Dispatch / retry queue for notification tasks.
#[cfg_attr(feature = "mocks", mockall::automock)]
#[async_trait]
pub trait MessageQueueTrait: Send + Sync {
    async fn publish_dispatch(&self, notification_id: Uuid) -> Result<()>;
    async fn publish_retry(&self, notification_id: Uuid, delay_ms: u32) -> Result<()>;
}

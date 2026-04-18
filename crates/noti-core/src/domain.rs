//! Domain entities for the notification service.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Type;
use uuid::Uuid;

/// The delivery channel for a notification.
#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[sqlx(type_name = "notification_channel", rename_all = "lowercase")]
pub enum NotificationChannel {
    Email,
    Sms,
    Push,
    Webhook,
    WebSocket,
}

/// Lifecycle status of a notification.
#[derive(Debug, Clone, Serialize, Deserialize, Type, PartialEq, Eq)]
#[sqlx(type_name = "notification_status", rename_all = "snake_case")]
pub enum NotificationStatus {
    Pending,
    Processing,
    Sent,
    Delivered,
    Failed,
    PermanentFailure,
}

/// A persisted notification record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub id: Uuid,
    pub user_id: Option<Uuid>,
    pub channel: NotificationChannel,
    pub status: NotificationStatus,
    pub recipient: String,
    pub template_id: String,
    pub variables: serde_json::Value,
    pub provider_id: Option<String>,
    pub provider_ref: Option<String>,
    pub retry_count: i32,
    pub next_retry_at: DateTime<Utc>,
    pub error_message: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
}

//! Domain entities for the notification service.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// The delivery channel for a notification.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum NotificationChannel {
    Email,
    Sms,
    Push,
    Webhook,
    WebSocket,
}

/// Lifecycle status of a notification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum NotificationStatus {
    Pending,
    Processing,
    Sent,
    Delivered,
    Failed,
    PermanentFailure,
}

/// Platform a push device token targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum DevicePlatform {
    Android,
    Ios,
    Web,
}

impl DevicePlatform {
    /// Lowercase wire/storage representation.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Android => "android",
            Self::Ios => "ios",
            Self::Web => "web",
        }
    }

    /// Parse from the lowercase storage representation.
    #[must_use]
    pub fn from_db(s: &str) -> Option<Self> {
        match s {
            "android" => Some(Self::Android),
            "ios" => Some(Self::Ios),
            "web" => Some(Self::Web),
            _ => None,
        }
    }
}

/// A registered push device token for a user (mobile or web).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeviceToken {
    pub id: Uuid,
    pub user_id: Uuid,
    pub token: String,
    pub platform: DevicePlatform,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

/// A persisted notification record.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Notification {
    pub id: Uuid,
    pub user_id: Option<Uuid>,
    pub channel: NotificationChannel,
    pub status: NotificationStatus,
    pub recipient: String,
    pub template_id: String,
    #[schema(value_type = Object)]
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
    pub read_at: Option<DateTime<Utc>>,
}

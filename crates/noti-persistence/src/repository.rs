//! PostgreSQL-backed notification repository.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::Result;
use noti_core::traits::NotificationRepositoryTrait;

pub struct NotificationRepository {
    pool: PgPool,
}

impl NotificationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NotificationRow {
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
    pub read_at: Option<DateTime<Utc>>,
}

impl NotificationRow {
    pub fn into_domain(self) -> Notification {
        Notification {
            id: self.id,
            user_id: self.user_id,
            channel: self.channel,
            status: self.status,
            recipient: self.recipient,
            template_id: self.template_id,
            variables: self.variables,
            provider_id: self.provider_id,
            provider_ref: self.provider_ref,
            retry_count: self.retry_count,
            next_retry_at: self.next_retry_at,
            error_message: self.error_message,
            idempotency_key: self.idempotency_key,
            created_at: self.created_at,
            updated_at: self.updated_at,
            sent_at: self.sent_at,
            read_at: self.read_at,
        }
    }
}

#[async_trait]
impl NotificationRepositoryTrait for NotificationRepository {
    async fn create(&self, n: &Notification) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO notifications (
                id, user_id, channel, status, recipient, template_id, variables,
                idempotency_key, next_retry_at, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(n.id)
        .bind(n.user_id)
        .bind(n.channel.clone())
        .bind(n.status.clone())
        .bind(&n.recipient)
        .bind(&n.template_id)
        .bind(&n.variables)
        .bind(&n.idempotency_key)
        .bind(n.next_retry_at)
        .bind(n.created_at)
        .bind(n.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<Notification>> {
        let res = sqlx::query_as::<_, NotificationRow>(
            r#"
            SELECT
                id, user_id, channel, status, recipient, template_id, variables,
                provider_id, provider_ref, retry_count, next_retry_at, error_message,
                idempotency_key, created_at, updated_at, sent_at, read_at
            FROM notifications
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(res.map(|r| r.into_domain()))
    }

    async fn get_by_idempotency_key(&self, key: &str) -> Result<Option<Notification>> {
        let res = sqlx::query_as::<_, NotificationRow>(
            r#"
            SELECT
                id, user_id, channel, status, recipient, template_id, variables,
                provider_id, provider_ref, retry_count, next_retry_at, error_message,
                idempotency_key, created_at, updated_at, sent_at, read_at
            FROM notifications
            WHERE idempotency_key = $1
            "#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(res.map(|r| r.into_domain()))
    }

    async fn update_status(
        &self,
        id: Uuid,
        status: NotificationStatus,
        error: Option<String>,
        provider_ref: Option<String>,
    ) -> Result<()> {
        let sent_at = if status == NotificationStatus::Sent {
            Some(Utc::now())
        } else {
            None
        };

        sqlx::query(
            r#"
            UPDATE notifications
            SET status = $2, error_message = $3, provider_ref = $4,
                sent_at = COALESCE($5, sent_at), updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(status)
        .bind(error)
        .bind(provider_ref)
        .bind(sent_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn increment_retry(&self, id: Uuid, next_retry_at: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE notifications
            SET retry_count = retry_count + 1, next_retry_at = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(next_retry_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_pending_for_retry(&self, limit: i32) -> Result<Vec<Notification>> {
        let res = sqlx::query_as::<_, NotificationRow>(
            r#"
            SELECT
                id, user_id, channel, status, recipient, template_id, variables,
                provider_id, provider_ref, retry_count, next_retry_at, error_message,
                idempotency_key, created_at, updated_at, sent_at, read_at
            FROM notifications
            WHERE status = 'pending' AND next_retry_at <= NOW()
            LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;

        Ok(res.into_iter().map(|r| r.into_domain()).collect())
    }

    async fn list_by_user(&self, user_id: Uuid, limit: i64, offset: i64) -> Result<Vec<Notification>> {
        let res = sqlx::query_as::<_, NotificationRow>(
            r#"
            SELECT
                id, user_id, channel, status, recipient, template_id, variables,
                provider_id, provider_ref, retry_count, next_retry_at, error_message,
                idempotency_key, created_at, updated_at, sent_at, read_at
            FROM notifications
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(res.into_iter().map(|r| r.into_domain()).collect())
    }

    async fn mark_as_read(&self, id: Uuid, user_id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE notifications
            SET read_at = NOW(), updated_at = NOW()
            WHERE id = $1 AND user_id = $2 AND read_at IS NULL
            "#,
        )
        .bind(id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_all_as_read(&self, user_id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE notifications
            SET read_at = NOW(), updated_at = NOW()
            WHERE user_id = $1 AND read_at IS NULL
            "#,
        )
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_unread_count(&self, user_id: Uuid) -> Result<i64> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND read_at IS NULL"
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.0)
    }
}

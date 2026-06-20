//! PostgreSQL-backed push device-token registry.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use noti_core::domain::{DevicePlatform, DeviceToken};
use noti_core::error::{NotiError, Result};
use noti_core::traits::DeviceTokenRepositoryTrait;

#[derive(Clone)]
pub struct PgDeviceTokenRepository {
    pool: PgPool,
}

impl PgDeviceTokenRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Raw row; `platform` is stored as TEXT (CHECK-constrained) and mapped to the
/// domain enum at the adapter boundary, keeping `noti-core` free of `SQLx`.
#[derive(sqlx::FromRow)]
struct DeviceTokenRow {
    id: Uuid,
    user_id: Uuid,
    token: String,
    platform: String,
    created_at: DateTime<Utc>,
    last_seen_at: DateTime<Utc>,
}

impl DeviceTokenRow {
    fn into_domain(self) -> Result<DeviceToken> {
        let platform = DevicePlatform::from_db(&self.platform).ok_or_else(|| {
            NotiError::Internal(format!("unknown device platform in DB: {}", self.platform))
        })?;
        Ok(DeviceToken {
            id: self.id,
            user_id: self.user_id,
            token: self.token,
            platform,
            created_at: self.created_at,
            last_seen_at: self.last_seen_at,
        })
    }
}

#[async_trait]
impl DeviceTokenRepositoryTrait for PgDeviceTokenRepository {
    async fn register(
        &self,
        user_id: Uuid,
        token: &str,
        platform: DevicePlatform,
    ) -> Result<DeviceToken> {
        // Tokens are globally unique (one physical device → one token). Upsert on
        // the token so a device that re-registers under a new user, or one whose
        // token was revoked, is reactivated and re-pointed atomically.
        let row = sqlx::query_as::<_, DeviceTokenRow>(
            r"
            INSERT INTO device_tokens (user_id, token, platform)
            VALUES ($1, $2, $3)
            ON CONFLICT (token) DO UPDATE
            SET user_id = EXCLUDED.user_id,
                platform = EXCLUDED.platform,
                last_seen_at = NOW(),
                revoked_at = NULL
            RETURNING id, user_id, token, platform, created_at, last_seen_at
            ",
        )
        .bind(user_id)
        .bind(token)
        .bind(platform.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(NotiError::database)?;

        row.into_domain()
    }

    async fn active_for_user(&self, user_id: Uuid) -> Result<Vec<DeviceToken>> {
        let rows = sqlx::query_as::<_, DeviceTokenRow>(
            r"
            SELECT id, user_id, token, platform, created_at, last_seen_at
            FROM device_tokens
            WHERE user_id = $1 AND revoked_at IS NULL
            ",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(NotiError::database)?;

        rows.into_iter().map(DeviceTokenRow::into_domain).collect()
    }

    async fn revoke(&self, token: &str) -> Result<()> {
        sqlx::query(
            r"
            UPDATE device_tokens
            SET revoked_at = NOW()
            WHERE token = $1 AND revoked_at IS NULL
            ",
        )
        .bind(token)
        .execute(&self.pool)
        .await
        .map_err(NotiError::database)?;

        Ok(())
    }
}

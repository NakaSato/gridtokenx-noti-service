//! Redis-backed cache service implementing `CacheTrait`.

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::{Client, aio::ConnectionManager};
use tracing::info;

use noti_core::error::{NotiError, Result};
use noti_core::traits::CacheTrait;

pub const DEFAULT_TTL: u64 = 300; // 5 minutes

#[derive(Clone)]
pub struct CacheService {
    conn: ConnectionManager,
    prefix: String,
}

impl CacheService {
    pub async fn new(redis_url: &str, prefix: &str) -> std::result::Result<Self, anyhow::Error> {
        let client =
            Client::open(redis_url).map_err(|e| anyhow::anyhow!("Redis client error: {e}"))?;

        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| anyhow::anyhow!("Redis connection error: {e}"))?;

        info!("✅ Redis cache service connected (prefix: {})", prefix);

        Ok(Self {
            conn,
            prefix: prefix.to_string(),
        })
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}:{}", self.prefix, key)
    }
}

#[async_trait]
impl CacheTrait for CacheService {
    async fn set_value(&self, key: &str, value: serde_json::Value, ttl_secs: u64) -> Result<()> {
        let serialized = value.to_string();
        let mut conn = self.conn.clone();
        let full_key = self.full_key(key);
        let _: () = conn
            .set_ex(full_key, serialized, ttl_secs)
            .await
            .map_err(|e| NotiError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_value(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let mut conn = self.conn.clone();
        let full_key = self.full_key(key);
        let raw: Option<String> = conn
            .get(full_key)
            .await
            .map_err(|e| NotiError::Internal(e.to_string()))?;

        match raw {
            Some(val) => Ok(Some(
                serde_json::from_str(&val).map_err(|e| NotiError::Internal(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }

    async fn increment_with_ttl(&self, key: &str, ttl_secs: u64) -> Result<i64> {
        let mut conn = self.conn.clone();
        let full_key = self.full_key(key);
        let mut pipe = redis::pipe();
        let (val,): (i64,) = pipe
            .atomic()
            .incr(&full_key, 1)
            .expire(&full_key, ttl_secs as i64)
            .ignore()
            .incr(&full_key, 0)
            .query_async(&mut conn)
            .await
            .map_err(|e| NotiError::Internal(e.to_string()))?;
        Ok(val)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let full_key = self.full_key(key);
        let _: () = conn
            .del(full_key)
            .await
            .map_err(|e| NotiError::Internal(e.to_string()))?;
        Ok(())
    }
}

/// Well-known cache key constructors.
pub mod keys {
    pub fn idempotency(key: &str) -> String {
        format!("idempotency:{key}")
    }

    pub fn rate_limit(channel: &str, recipient: &str) -> String {
        format!("rate_limit:{channel}:{recipient}")
    }

    pub fn template(template_id: &str) -> String {
        format!("template:{template_id}")
    }

    pub fn ws_registry(user_id: &str) -> String {
        format!("ws_registry:{user_id}")
    }
}

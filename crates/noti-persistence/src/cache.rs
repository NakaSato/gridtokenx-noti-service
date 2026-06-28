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
    /// # Errors
    ///
    /// Returns an error if the Redis client cannot be created or the
    /// connection manager fails to establish a connection.
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
            Some(val) => {
                let parsed =
                    serde_json::from_str(&val).map_err(|e| NotiError::Internal(e.to_string()))?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    async fn increment_with_ttl(&self, key: &str, ttl_secs: u64) -> Result<i64> {
        let mut conn = self.conn.clone();
        let full_key = self.full_key(key);
        let val: i64 = redis::Script::new(
            r"
            local value = redis.call('INCR', KEYS[1])
            redis.call('EXPIRE', KEYS[1], ARGV[1])
            return value
            ",
        )
        .key(full_key)
        .arg(ttl_secs)
        .invoke_async(&mut conn)
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

    async fn lock(&self, key: &str, ttl_secs: u64) -> Result<bool> {
        let mut conn = self.conn.clone();
        let full_key = self.full_key(key);
        let result: Option<String> = redis::cmd("SET")
            .arg(&full_key)
            .arg("locked")
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| NotiError::Internal(e.to_string()))?;
        Ok(result.is_some())
    }

    async fn unlock(&self, key: &str) -> Result<()> {
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
    #[must_use]
    pub fn idempotency(key: &str) -> String {
        format!("idempotency:{key}")
    }

    #[must_use]
    pub fn rate_limit(channel: &str, recipient: &str) -> String {
        format!("rate_limit:{channel}:{recipient}")
    }

    #[must_use]
    pub fn template(template_id: &str) -> String {
        format!("template:{template_id}")
    }

    #[must_use]
    pub fn ws_registry(user_id: &str) -> String {
        format!("ws_registry:{user_id}")
    }
}

#[cfg(test)]
mod tests {
    use super::keys;

    #[test]
    fn key_formats_are_stable() {
        // Redis-key contract: a silent format change orphans existing keys
        // (idempotency replays double-send, rate limits reset, templates miss).
        assert_eq!(keys::idempotency("evt-1"), "idempotency:evt-1");
        assert_eq!(keys::template("welcome"), "template:welcome");
        assert_eq!(keys::ws_registry("uid-1"), "ws_registry:uid-1");
    }

    #[test]
    fn rate_limit_orders_channel_then_recipient() {
        // (channel, recipient) — channel-first; a swap would merge unrelated
        // limits (e.g. email vs sms to the same address).
        assert_eq!(
            keys::rate_limit("email", "u@x.io"),
            "rate_limit:email:u@x.io"
        );
        assert_ne!(
            keys::rate_limit("email", "sms"),
            keys::rate_limit("sms", "email")
        );
    }
}

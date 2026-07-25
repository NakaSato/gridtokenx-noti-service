//! Cache-backed counters used by ingestion to suppress repeat notifications.
//!
//! Event handlers sometimes need "have I seen this before?" state that is not a
//! notification itself — e.g. whether a login IP is new to a user. Exposing a
//! counter keeps the cache handle private to the orchestrator while letting the
//! Kafka consumers make that decision.

use noti_core::error::Result;

use super::NotificationOrchestrator;

impl NotificationOrchestrator {
    /// Increment the rolling counter at `key` (creating it with a `ttl_secs`
    /// expiry) and return its new value. A return of `1` means the key was not
    /// present — i.e. this is the first occurrence within the TTL window.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache is unreachable.
    pub async fn bump_counter(&self, key: &str, ttl_secs: u64) -> Result<i64> {
        self.cache.increment_with_ttl(key, ttl_secs).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use noti_core::traits::{
        MockCacheTrait, MockNotificationProviderTrait, MockNotificationRepositoryTrait,
        MockTemplateEngineTrait,
    };

    use super::NotificationOrchestrator;

    #[tokio::test]
    async fn bump_counter_returns_cache_value() {
        let mut cache = MockCacheTrait::new();
        cache
            .expect_increment_with_ttl()
            .times(1)
            .returning(|_, _| Ok(3));

        let orch = NotificationOrchestrator::new(
            Arc::new(MockNotificationRepositoryTrait::new()),
            Arc::new(MockTemplateEngineTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(cache),
            None,
        );

        assert_eq!(orch.bump_counter("k", 60).await.expect("bump"), 3);
    }
}

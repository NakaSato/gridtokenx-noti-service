//! Integration tests for `CacheService` against a real Redis.
//!
//! Closes gap #5 (the cache had zero coverage). The lock/unlock and
//! increment-with-ttl paths back idempotency and rate-limiting, where a subtle
//! bug means silently duplicated or dropped notifications. Requires a
//! Docker/OrbStack daemon.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use noti_core::traits::CacheTrait;
use noti_persistence::cache::CacheService;

use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Boot a Redis container and build a `CacheService` with the given key prefix.
async fn setup(prefix: &str) -> (ContainerAsync<Redis>, CacheService) {
    let container = Redis::default().start().await.expect("start redis container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("container port");
    let url = format!("redis://{host}:{port}");
    let cache = CacheService::new(&url, prefix).await.expect("connect cache");
    (container, cache)
}

#[tokio::test]
async fn cache_value_and_counter_and_lock() {
    let (_redis, cache) = setup("noti-test").await;

    // --- value set / get / delete -----------------------------------------
    assert!(
        cache.get_value("missing").await.unwrap().is_none(),
        "absent key is None"
    );
    cache
        .set_value("greeting", serde_json::json!({"hi": "there"}), 60)
        .await
        .expect("set_value");
    assert_eq!(
        cache.get_value("greeting").await.unwrap().unwrap(),
        serde_json::json!({"hi": "there"})
    );
    cache.delete("greeting").await.expect("delete");
    assert!(cache.get_value("greeting").await.unwrap().is_none());

    // --- atomic increment-with-ttl (rate limiting) ------------------------
    assert_eq!(cache.increment_with_ttl("counter", 60).await.unwrap(), 1);
    assert_eq!(cache.increment_with_ttl("counter", 60).await.unwrap(), 2);
    assert_eq!(cache.increment_with_ttl("counter", 60).await.unwrap(), 3);

    // --- NX lock semantics (idempotency guard) ----------------------------
    assert!(cache.lock("job-1", 60).await.unwrap(), "first lock acquires");
    assert!(
        !cache.lock("job-1", 60).await.unwrap(),
        "second lock on a held key must fail (SET NX)"
    );
    cache.unlock("job-1").await.expect("unlock");
    assert!(
        cache.lock("job-1", 60).await.unwrap(),
        "lock re-acquirable after unlock"
    );
}

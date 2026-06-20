//! Integration tests for `PgDeviceTokenRepository` against a real `PostgreSQL`.
//!
//! The repository's queries are runtime-checked (`query_as::<_, T>`, not the
//! compile-time macros), so a schema/column drift — or a botched upsert/partial
//! index — would only surface in production. This spins one ephemeral Postgres
//! container (testcontainers), runs the workspace migrations (including
//! `device_tokens`), and exercises register / upsert-repoint / revoke /
//! reactivate through the trait.
//!
//! Requires a Docker/OrbStack daemon — without one the container fails to start
//! and the test fails loudly rather than silently skipping.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use sqlx::PgPool;
use uuid::Uuid;

use noti_core::domain::DevicePlatform;
use noti_core::traits::DeviceTokenRepositoryTrait;
use noti_persistence::device_tokens::PgDeviceTokenRepository;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Boot a Postgres container, connect a pool, run migrations, return the repo.
/// The container guard must stay in scope for the pool to remain valid.
/// Pinned to PG16 so the schema's `gen_random_uuid()` is a core builtin.
async fn setup() -> (ContainerAsync<Postgres>, PgDeviceTokenRepository) {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = PgPool::connect(&url).await.expect("connect pool");
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    (container, PgDeviceTokenRepository::new(pool))
}

/// One container, exercising the registry surface end to end: register,
/// list-active, upsert-repoint, revoke (soft-delete), and reactivate.
#[tokio::test]
async fn device_token_lifecycle() {
    let (_pg, repo) = setup().await;

    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();

    // --- Phase 1: register two devices for Alice ---------------------------
    let android = repo
        .register(alice, "tok-android", DevicePlatform::Android)
        .await
        .expect("register android");
    assert_eq!(android.user_id, alice);
    assert_eq!(android.platform, DevicePlatform::Android);
    assert_eq!(android.token, "tok-android");

    repo.register(alice, "tok-web", DevicePlatform::Web)
        .await
        .expect("register web");

    let active = repo.active_for_user(alice).await.expect("active");
    assert_eq!(active.len(), 2, "both of Alice's devices are active");

    // --- Phase 2: upsert on token re-points to a new user atomically -------
    // The same physical device (token) now belongs to Bob. ON CONFLICT(token)
    // must move ownership, not create a second row.
    let repointed = repo
        .register(bob, "tok-web", DevicePlatform::Web)
        .await
        .expect("repoint web token");
    assert_eq!(repointed.user_id, bob, "token re-pointed to Bob");

    let alice_active = repo.active_for_user(alice).await.expect("alice active");
    assert_eq!(alice_active.len(), 1, "web token left Alice");
    assert_eq!(alice_active[0].token, "tok-android");
    let bob_active = repo.active_for_user(bob).await.expect("bob active");
    assert_eq!(bob_active.len(), 1);
    assert_eq!(bob_active[0].token, "tok-web");

    // --- Phase 3: revoke is a soft-delete (drops from active set) -----------
    repo.revoke("tok-android").await.expect("revoke android");
    assert!(
        repo.active_for_user(alice).await.expect("alice active").is_empty(),
        "revoked token no longer active"
    );

    // Revoking an unknown / already-revoked token is a no-op, not an error.
    repo.revoke("tok-android").await.expect("re-revoke is no-op");
    repo.revoke("does-not-exist").await.expect("revoke missing is no-op");

    // --- Phase 4: re-registering a revoked token reactivates it ------------
    let revived = repo
        .register(alice, "tok-android", DevicePlatform::Android)
        .await
        .expect("re-register revoked token");
    assert_eq!(revived.user_id, alice);
    let alice_active = repo.active_for_user(alice).await.expect("alice active");
    assert_eq!(alice_active.len(), 1, "token reactivated for Alice");
    assert_eq!(alice_active[0].token, "tok-android");
}

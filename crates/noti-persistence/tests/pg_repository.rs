//! Integration tests for `NotificationRepository` against a real `PostgreSQL`.
//!
//! Spins one ephemeral Postgres container (testcontainers), runs the workspace
//! migrations, and exercises the repository through its trait. Closes gap #4
//! (the repo had zero coverage; its queries are runtime-checked, so a
//! schema/column drift would only surface in production without these).
//!
//! A single container is shared across phases to keep CI fast. Requires a
//! Docker/OrbStack daemon — without one the container fails to start and the
//! test fails loudly rather than silently skipping.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::{Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::traits::NotificationRepositoryTrait;
use noti_persistence::repository::NotificationRepository;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Boot a Postgres container, connect a pool, and run migrations. Returns the
/// container guard (must stay in scope) and a repo wired with that pool.
/// Pinned to PG16 so the schema's `gen_random_uuid()` is a core builtin.
async fn setup() -> (ContainerAsync<Postgres>, NotificationRepository) {
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

    // The repo splits reads/writes across two pools; one pool is fine for tests.
    let repo = NotificationRepository::new(pool.clone(), pool);
    (container, repo)
}

/// A minimal `Email` notification with the given id / idempotency key.
fn sample(id: Uuid, user_id: Option<Uuid>, idempotency_key: Option<String>) -> Notification {
    let now = Utc::now();
    Notification {
        id,
        user_id,
        channel: NotificationChannel::Email,
        status: NotificationStatus::Pending,
        recipient: "user@example.com".to_string(),
        template_id: "welcome.html.tera".to_string(),
        variables: serde_json::json!({"name": "Ada"}),
        provider_id: None,
        provider_ref: None,
        retry_count: 0,
        next_retry_at: now,
        error_message: None,
        idempotency_key,
        created_at: now,
        updated_at: now,
        sent_at: None,
        read_at: None,
    }
}

/// One container, three sequential phases covering the repository surface:
/// create + idempotency, status/retry/read tracking, and pending/reset.
#[tokio::test]
async fn repository_lifecycle() {
    let (_pg, repo) = setup().await;

    // --- Phase 1: create roundtrip + idempotent ON CONFLICT ----------------
    let id = Uuid::new_v4();
    let user = Uuid::new_v4();
    let key = "idem-key-1".to_string();
    let created = repo
        .create(&sample(id, Some(user), Some(key.clone())))
        .await
        .expect("create");
    assert_eq!(created.id, id);
    assert!(matches!(created.channel, NotificationChannel::Email));
    assert_eq!(created.status, NotificationStatus::Pending);

    let fetched = repo.get_by_id(id).await.unwrap().expect("row present");
    assert_eq!(fetched.recipient, "user@example.com");
    assert_eq!(fetched.variables, serde_json::json!({"name": "Ada"}));

    // Re-create with the SAME idempotency key but a fresh id: ON CONFLICT must
    // keep the original row rather than insert a duplicate.
    let dup = sample(Uuid::new_v4(), Some(Uuid::new_v4()), Some(key.clone()));
    let dup_created = repo.create(&dup).await.expect("create duplicate");
    assert_eq!(dup_created.id, id, "idempotency key must dedupe to original");
    let by_key = repo
        .get_by_idempotency_key(&key)
        .await
        .unwrap()
        .expect("row present");
    assert_eq!(by_key.id, id);

    // --- Phase 2: status / retry / read tracking ---------------------------
    repo.update_status(id, NotificationStatus::Sent, None, Some("smtp-123".into()))
        .await
        .expect("update_status");
    let after = repo.get_by_id(id).await.unwrap().unwrap();
    assert_eq!(after.status, NotificationStatus::Sent);
    assert_eq!(after.provider_ref.as_deref(), Some("smtp-123"));
    assert!(after.sent_at.is_some(), "Sent must set sent_at");

    let next = Utc::now() + Duration::minutes(5);
    repo.increment_retry(id, next).await.expect("increment_retry");
    assert_eq!(repo.get_by_id(id).await.unwrap().unwrap().retry_count, 1);

    assert_eq!(repo.get_unread_count(user).await.unwrap(), 1);
    repo.mark_as_read(id, user).await.expect("mark_as_read");
    assert_eq!(repo.get_unread_count(user).await.unwrap(), 0);
    assert!(repo.get_by_id(id).await.unwrap().unwrap().read_at.is_some());

    let listed = repo.list_by_user(user, 10, 0).await.expect("list_by_user");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);

    // --- Phase 3: pending-for-retry + reset-stuck-processing ---------------
    let due = Uuid::new_v4();
    let mut n = sample(due, None, None);
    n.next_retry_at = Utc::now() - Duration::minutes(1);
    repo.create(&n).await.expect("create due");

    let pending = repo.get_pending_for_retry(10).await.expect("pending");
    assert!(pending.iter().any(|p| p.id == due), "due row must be pending");

    repo.update_status(due, NotificationStatus::Processing, None, None)
        .await
        .expect("to processing");
    let reset = repo
        .reset_stuck_processing(0)
        .await
        .expect("reset_stuck_processing");
    assert!(reset >= 1, "at least the stuck row should reset");
    assert_eq!(
        repo.get_by_id(due).await.unwrap().unwrap().status,
        NotificationStatus::Pending
    );
}

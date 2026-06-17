//! Live test for the `RabbitMQ` dispatch consumer loop (runtime leg of gap #7).
//!
//! The broker-free unit tests cover the ack/nack *decision*
//! (`process_dispatch_payload`); `tests/rabbitmq_dlx.rs` (in noti-persistence)
//! covers the `noti.retry` DLX redelivery topology. This closes the leg left
//! UNVERIFIED: the production `start_rabbitmq_consumer` wired to a real broker
//! actually pulls a published task off `noti.dispatch`, deserializes it, and
//! invokes `orchestrator.dispatch` — and keeps consuming after a delivery that
//! fails dispatch (nack) rather than wedging.
//!
//! Requires a Docker/OrbStack daemon — panics (does not skip) without one.
//!
//! NOTE: `start_rabbitmq_consumer` only re-checks its cancellation token when a
//! delivery arrives (it parks in `consumer.next()` while idle), so the test
//! aborts the consumer task rather than awaiting a graceful stop. The idle-stop
//! gap is a separate follow-up, not in scope here.

#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::duration_suboptimal_units)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::traits::{
    MessageQueueTrait, MockCacheTrait, MockNotificationProviderTrait,
    MockNotificationRepositoryTrait, MockTemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::messaging::rabbitmq::RabbitMQClient;
use noti_server::consumers::start_rabbitmq_consumer;

use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

type Ids = Arc<Mutex<Vec<Uuid>>>;

fn pending(id: Uuid) -> Notification {
    let now = gridtokenx_telemetry::time::now();
    Notification {
        id,
        user_id: Some(Uuid::new_v4()),
        channel: NotificationChannel::Email,
        status: NotificationStatus::Pending,
        recipient: "user@example.com".into(),
        template_id: "welcome.html.tera".into(),
        variables: serde_json::json!({}),
        provider_id: None,
        provider_ref: None,
        retry_count: 0,
        next_retry_at: now,
        error_message: None,
        idempotency_key: None,
        created_at: now,
        updated_at: now,
        sent_at: None,
        read_at: None,
    }
}

/// Orchestrator recording every id dispatch looks up. `ok_id` resolves to a
/// pending row (dispatch succeeds → ack); every other id is missing (dispatch
/// returns `NotFound` → nack-drop).
fn recording_orchestrator(ok_id: Uuid) -> (Arc<NotificationOrchestrator>, Ids) {
    let seen: Ids = Arc::new(Mutex::new(Vec::new()));
    let recorder = seen.clone();

    let mut repo = MockNotificationRepositoryTrait::new();
    repo.expect_get_by_id().times(..).returning(move |id| {
        recorder.lock().expect("seen lock").push(id);
        if id == ok_id {
            Ok(Some(pending(id)))
        } else {
            Ok(None)
        }
    });
    repo.expect_update_status()
        .times(..)
        .returning(|_, _, _, _| Ok(()));

    let mut template = MockTemplateEngineTrait::new();
    template.expect_render().times(..).returning(|_, _| Ok("body".into()));

    let mut email = MockNotificationProviderTrait::new();
    email.expect_send().times(..).returning(|_, _| Ok("ref".into()));
    email.expect_provider_id().times(..).returning(|| "mock-email");

    let orch = Arc::new(NotificationOrchestrator::new(
        Arc::new(repo),
        Arc::new(template),
        Arc::new(email),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(MockCacheTrait::new()),
        None,
    ));
    (orch, seen)
}

async fn setup() -> (ContainerAsync<RabbitMq>, RabbitMQClient) {
    let container = RabbitMq::default()
        .start()
        .await
        .expect("start rabbitmq container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("container port");
    let url = format!("amqp://guest:guest@{host}:{port}");
    let client = RabbitMQClient::new(&url).await.expect("connect rabbitmq");
    (container, client)
}

/// Poll `seen` until it contains `id`, failing the test on timeout.
async fn wait_for(seen: &Ids, id: Uuid) {
    let found = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if seen.lock().expect("seen lock").contains(&id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(found.is_ok(), "consumer never dispatched {id} within 20s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_dispatches_published_tasks_and_survives_a_failure() {
    let (_mq, client) = setup().await;
    let client = Arc::new(client);

    let ok_id = Uuid::new_v4();
    let (orch, seen) = recording_orchestrator(ok_id);
    let token = CancellationToken::new();

    let consumer = tokio::spawn(start_rabbitmq_consumer(
        client.clone(),
        orch,
        token.clone(),
    ));

    // 1. A task whose row is missing → dispatch returns NotFound → the loop
    //    nacks (requeue:false) and drops it. Prove dispatch actually ran.
    let fail_id = Uuid::new_v4();
    client.publish_dispatch(fail_id).await.expect("publish fail task");
    wait_for(&seen, fail_id).await;

    // 2. A second, deliverable task after the failure → proves the consumer
    //    survived the nack and is still pulling from noti.dispatch.
    client.publish_dispatch(ok_id).await.expect("publish ok task");
    wait_for(&seen, ok_id).await;

    // start_rabbitmq_consumer parks in consumer.next() while idle, so cancel
    // can't unblock it without another delivery — abort instead of awaiting.
    token.cancel();
    consumer.abort();
    let _ = consumer.await;
}

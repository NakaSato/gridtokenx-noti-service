//! Integration tests for `RabbitMQClient` topology against a real `RabbitMQ`.
//!
//! Closes gap #6. Verifies two things the publisher relies on:
//!   1. `publish_dispatch` reaches the `noti.dispatch` queue (exchange +
//!      binding + routing key are declared correctly).
//!   2. The `noti.retry` queue dead-letters expired messages back to
//!      `noti.dispatch` via the `x-dead-letter-*` args — the TTL-backoff retry
//!      mechanism. A short message TTL is used so the test runs in seconds.
//!
//! One container, sequential phases. Requires a Docker/OrbStack daemon.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use futures::StreamExt;
use lapin::BasicProperties;
use lapin::options::{BasicConsumeOptions, BasicPublishOptions};
use lapin::types::{FieldTable, ShortString};
use uuid::Uuid;

use noti_core::traits::MessageQueueTrait;
use noti_persistence::messaging::rabbitmq::{NotificationTask, RabbitMQClient};

use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Boot a `RabbitMQ` container and build a client (which declares the topology).
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

/// Await the next task on a consumer, failing the test on timeout.
async fn next_task(consumer: &mut lapin::Consumer) -> NotificationTask {
    let delivery = tokio::time::timeout(Duration::from_secs(10), consumer.next())
        .await
        .expect("timed out waiting for a message")
        .expect("consumer stream ended unexpectedly")
        .expect("delivery error");
    serde_json::from_slice(&delivery.data).expect("decode NotificationTask")
}

#[tokio::test]
async fn dispatch_publish_and_retry_dead_letter() {
    let (_mq, client) = setup().await;

    let mut consumer = client
        .channel()
        .basic_consume(
            "noti.dispatch",
            "test-consumer",
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("consume noti.dispatch");

    // --- Phase 1: publish_dispatch is consumable on noti.dispatch ----------
    let dispatch_id = Uuid::new_v4();
    client
        .publish_dispatch(dispatch_id)
        .await
        .expect("publish_dispatch");
    let task = next_task(&mut consumer).await;
    assert_eq!(task.notification_id, dispatch_id);
    assert_eq!(task.retry_count, 0);

    // --- Phase 2: noti.retry dead-letters to noti.dispatch on TTL expiry ---
    // Publish straight onto the retry queue with a 300ms message TTL; the
    // queue's x-dead-letter-* args must route it to noti.dispatch on expiry.
    let retry_id = Uuid::new_v4();
    let payload = serde_json::to_vec(&NotificationTask {
        notification_id: retry_id,
        retry_count: 2,
    })
    .unwrap();
    client
        .channel()
        .basic_publish(
            "",
            "noti.retry",
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default().with_expiration(ShortString::from("300")),
        )
        .await
        .expect("publish to noti.retry");

    let dead_lettered = next_task(&mut consumer).await;
    assert_eq!(
        dead_lettered.notification_id, retry_id,
        "expired retry message must dead-letter to noti.dispatch"
    );
    assert_eq!(dead_lettered.retry_count, 2);
}

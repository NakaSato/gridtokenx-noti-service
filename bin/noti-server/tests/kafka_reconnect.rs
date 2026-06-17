//! Live broker-bounce test for the supervised Kafka consumer (runtime leg of
//! gap #2).
//!
//! The supervisor control flow (reconnect, backoff bounds, cancellation,
//! health mark-down) is unit-tested with a fake session in `consumers::tests`.
//! This closes the leg those can't reach: that the *real* consumer survives an
//! actual broker outage and resumes consuming once it returns — whether
//! recovery happens via a supervised session rebuild or librdkafka's in-place
//! auto-recovery, the runtime guarantee is the same: no permanent wedge.
//!
//! Boots Kafka, proves a message flows through, freezes the broker container
//! (`pause`) to simulate an outage, thaws it (`unpause`), and asserts a message
//! produced *after* the outage is still dispatched. Pause (not stop/start) is
//! used deliberately: Docker reassigns the published host port on container
//! restart, which would strand the consumer's fixed bootstrap address; pause
//! freezes the broker process while keeping the port mapping intact.
//!
//! Requires a Docker/OrbStack daemon — panics (does not skip) without one.
//! Slow by nature (a broker freeze/thaw cycle plus reconnect latency).

#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::duration_suboptimal_units)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use noti_core::domain::Notification;
use noti_core::health::KafkaConsumerHealth;
use noti_core::traits::{
    MockCacheTrait, MockMessageQueueTrait, MockNotificationProviderTrait,
    MockNotificationRepositoryTrait, MockTemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_server::consumers::start_kafka_consumer;

use testcontainers_modules::kafka::apache;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const TOPIC: &str = "iam.user.events";

type Sink = Arc<Mutex<Vec<Notification>>>;

fn recording_orchestrator() -> (Arc<NotificationOrchestrator>, Sink) {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let recorder = sink.clone();

    let mut repo = MockNotificationRepositoryTrait::new();
    repo.expect_create().times(..).returning(move |n| {
        recorder.lock().expect("sink lock").push(n.clone());
        Ok(n.clone())
    });

    let mut cache = MockCacheTrait::new();
    cache.expect_set_value().times(..).returning(|_, _, _| Ok(()));

    let mut mq = MockMessageQueueTrait::new();
    mq.expect_publish_dispatch().times(..).returning(|_| Ok(()));

    let orch = Arc::new(NotificationOrchestrator::new(
        Arc::new(repo),
        Arc::new(MockTemplateEngineTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(MockNotificationProviderTrait::new()),
        Arc::new(cache),
        Some(Arc::new(mq)),
    ));
    (orch, sink)
}

fn producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("create producer")
}

fn event(email: &str) -> String {
    serde_json::json!({
        "event_type": "EmailVerified",
        "data": { "email": email, "username": "bounce" }
    })
    .to_string()
}

/// Produce `payload` (re-sending to ride out assignment/reconnect latency)
/// until the sink holds a notification addressed to `email`, or time out.
async fn produce_until_seen(brokers: &str, payload: &str, email: &str, sink: &Sink, secs: u64) {
    let producer = producer(brokers);
    let seen = tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            let _ = producer
                .send(
                    FutureRecord::<str, str>::to(TOPIC).payload(payload),
                    Duration::from_secs(5),
                )
                .await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            if sink
                .lock()
                .expect("lock")
                .iter()
                .any(|n| n.recipient == email)
            {
                break;
            }
        }
    })
    .await;
    assert!(seen.is_ok(), "'{email}' never dispatched within {secs}s");
}

async fn brokers_for(kafka: &ContainerAsync<apache::Kafka>) -> String {
    format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("kafka port")
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_resumes_after_broker_bounce() {
    let kafka = apache::Kafka::default()
        .start()
        .await
        .expect("start kafka container");
    let brokers = brokers_for(&kafka).await;

    let (orch, sink) = recording_orchestrator();
    let health = Arc::new(KafkaConsumerHealth::new());
    let token = CancellationToken::new();

    let consumer = tokio::spawn(start_kafka_consumer(
        brokers.clone(),
        format!("noti-reconnect-{}", Uuid::new_v4()),
        vec![TOPIC.to_string()],
        orch,
        None,
        health.clone(),
        token.clone(),
    ));

    // Phase 1: a healthy session dispatches a message.
    produce_until_seen(&brokers, &event("before@example.com"), "before@example.com", &sink, 60)
        .await;

    // Phase 2: freeze the broker for a few seconds (an outage), then thaw it.
    // The host port mapping survives a pause, so the consumer reconnects to
    // the same address it was given.
    kafka.pause().await.expect("pause broker");
    tokio::time::sleep(Duration::from_secs(5)).await;
    kafka.unpause().await.expect("unpause broker");
    assert_eq!(
        brokers_for(&kafka).await,
        brokers,
        "paused container kept its host port"
    );

    // Phase 3: a message produced after the outage must still be dispatched —
    // the consumer recovered rather than wedging permanently.
    produce_until_seen(&brokers, &event("after@example.com"), "after@example.com", &sink, 120)
        .await;

    token.cancel();
    consumer.abort();
    let _ = consumer.await;
}

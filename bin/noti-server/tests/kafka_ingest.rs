//! Live end-to-end test for the Kafka ingest path (the runtime half of gap #1).
//!
//! The unit tests in `consumers` cover envelope decoding and event→queue
//! mapping with no broker. This closes the remaining UNVERIFIED leg: the real
//! rdkafka transport. It boots an ephemeral Kafka, runs the *production*
//! `start_kafka_consumer`, produces a genuine event record with a real
//! `FutureProducer`, and asserts the consumer polled it, decoded the envelope,
//! and drove `orchestrator.queue_notification` through to `repo.create`.
//!
//! Requires a Docker/OrbStack daemon — panics (does not skip) without one,
//! matching the persistence integration tests.

#![allow(clippy::expect_used, clippy::unwrap_used)]
// `Duration::from_secs(60)` reads fine here and `from_mins` is too new to rely
// on across the CI toolchain.
#![allow(clippy::duration_suboptimal_units)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel};
use noti_core::health::KafkaConsumerHealth;
use noti_core::traits::{
    MockCacheTrait, MockMessageQueueTrait, MockNotificationProviderTrait,
    MockNotificationRepositoryTrait, MockTemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_server::consumers::start_kafka_consumer;

use testcontainers_modules::kafka::apache;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const TOPIC: &str = "iam.user.events";

type Sink = Arc<Mutex<Vec<Notification>>>;

/// Orchestrator whose `repo.create` records every queued notification; cache
/// and MQ accept everything (the queue path never renders or sends).
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_event_flows_through_to_queue_notification() {
    let kafka = apache::Kafka::default()
        .start()
        .await
        .expect("start kafka container");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("kafka port")
    );

    let (orch, sink) = recording_orchestrator();
    let health = Arc::new(KafkaConsumerHealth::new());
    let token = CancellationToken::new();

    // Run the *production* consumer against the live broker. `auto.offset.reset
    // = earliest` (see noti-persistence kafka::create_consumer) means a fresh
    // group still reads a record produced before assignment completes.
    let consumer_token = token.clone();
    let consumer = tokio::spawn(start_kafka_consumer(
        brokers.clone(),
        format!("noti-e2e-{}", Uuid::new_v4()),
        vec![TOPIC.to_string()],
        orch,
        Some("https://app.gridtokenx.test".to_string()),
        health,
        consumer_token,
    ));

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("create producer");

    let payload = serde_json::json!({
        "event_type": "EmailVerified",
        "data": { "email": "e2e@example.com", "username": "e2e-user" }
    })
    .to_string();

    // Produce repeatedly: the consumer group may still be (re)balancing when
    // the first record lands. Idempotency on the key collapses duplicates, so
    // re-sending is harmless and removes the assignment race.
    let key = "e2e-key";
    let queued = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            producer
                .send(
                    FutureRecord::to(TOPIC).payload(&payload).key(key),
                    Duration::from_secs(5),
                )
                .await
                .expect("produce event");

            tokio::time::sleep(Duration::from_millis(500)).await;
            if let Some(n) = sink.lock().expect("lock").first().cloned() {
                break n;
            }
        }
    })
    .await
    .expect("event never reached queue_notification within 60s");

    token.cancel();
    let _ = consumer.await;

    // Full path verified: poll → handle_record envelope decode → dispatch →
    // queue_notification → repo.create.
    assert!(matches!(queued.channel, NotificationChannel::Email));
    assert_eq!(queued.recipient, "e2e@example.com");
    assert_eq!(queued.template_id, "welcome.html.tera");
}

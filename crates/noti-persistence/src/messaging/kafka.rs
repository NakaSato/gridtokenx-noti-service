//! Kafka consumer configuration helper.
//!
//! The actual consumer loop lives in `noti-server` to avoid a circular
//! dependency with `noti-logic`. This module provides only the
//! `StreamConsumer` factory.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::StreamConsumer;

/// Create a configured `StreamConsumer` ready to subscribe.
pub fn create_consumer(brokers: &str, group_id: &str) -> anyhow::Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;

    Ok(consumer)
}

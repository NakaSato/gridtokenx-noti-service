//! Shared helpers for the `noti-server` container e2e tests.
//!
//! The pipeline/recovery tests each boot the same testcontainers and derive
//! connection strings the same way. Those URL builders were copy-pasted across
//! files; the canonical versions live here. Test-specific wiring (which
//! containers, which providers, the assertions) stays in each file.
//!
//! Compiled once per test binary, so a given file may not use every helper.

#![allow(dead_code)]

use testcontainers_modules::kafka::apache;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;

/// `postgres://` URL for a booted Postgres container.
pub async fn pg_url(c: &ContainerAsync<Postgres>) -> String {
    let host = c.get_host().await.expect("pg host");
    let port = c.get_host_port_ipv4(5432).await.expect("pg port");
    format!("postgres://postgres:postgres@{host}:{port}/postgres")
}

/// `redis://` URL for a booted Redis container.
pub async fn redis_url(c: &ContainerAsync<Redis>) -> String {
    let host = c.get_host().await.expect("redis host");
    let port = c.get_host_port_ipv4(6379).await.expect("redis port");
    format!("redis://{host}:{port}")
}

/// `amqp://` URL (default vhost) for a booted `RabbitMQ` container.
pub async fn rabbit_url(c: &ContainerAsync<RabbitMq>) -> String {
    let host = c.get_host().await.expect("rabbit host");
    let port = c.get_host_port_ipv4(5672).await.expect("rabbit port");
    format!("amqp://{host}:{port}/%2f")
}

/// Bootstrap-servers string for a booted Kafka container.
pub async fn kafka_brokers(c: &ContainerAsync<apache::Kafka>) -> String {
    let port = c
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("kafka port");
    format!("127.0.0.1:{port}")
}

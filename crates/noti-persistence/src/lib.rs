//! # noti-persistence
//!
//! Infrastructure adapters for the notification service:
//! `PostgreSQL` repositories, Redis cache, `Kafka`/`RabbitMQ` messaging,
//! notification providers, and template engine.

pub mod cache;
pub mod messaging;
pub mod providers;
pub mod repository;
pub mod templating;

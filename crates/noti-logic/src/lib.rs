//! # noti-logic
//!
//! Business orchestration for the notification service.
//! Depends only on trait contracts from `noti-core` — no concrete adapters.

pub mod orchestrator;

pub use orchestrator::NotificationOrchestrator;

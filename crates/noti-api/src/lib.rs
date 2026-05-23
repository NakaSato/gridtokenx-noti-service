//! # noti-api
//!
//! gRPC and REST handlers for the notification service.
//! Bridges protocol types to domain logic via `noti-logic`.

use noti_logic::NotificationOrchestrator;
use std::sync::Arc;

pub mod auth;
pub mod grpc;
pub mod handlers;
pub mod websocket;

#[derive(Clone)]
pub struct AppState {
    pub orchestrator: Arc<NotificationOrchestrator>,
}

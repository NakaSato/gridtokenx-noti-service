//! # noti-api
//!
//! gRPC and REST handlers for the notification service.
//! Bridges protocol types to domain logic via `noti-logic`.

use noti_core::traits::DeviceTokenRepositoryTrait;
use noti_logic::NotificationOrchestrator;
use std::sync::Arc;

pub mod auth;
pub mod grpc;
pub mod handlers;
pub mod openapi;
pub mod websocket;

#[derive(Clone)]
pub struct AppState {
    pub orchestrator: Arc<NotificationOrchestrator>,
    /// Push device-token registry, used by the device registration handlers.
    /// Pure CRUD, so handlers reach it directly rather than via the orchestrator.
    pub device_repo: Arc<dyn DeviceTokenRepositoryTrait>,
}

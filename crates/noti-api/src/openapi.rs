//! `OpenAPI` document for the notification service REST surface.
//!
//! Aggregates every `#[utoipa::path]`-annotated handler and the schemas they
//! reference into a single [`ApiDoc`]. The server mounts this as Swagger UI plus
//! a raw `openapi.json` (see `noti-server::startup`).

use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::handlers;

/// Injects the `bearer` (JWT) security scheme referenced by the authed paths.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        // `components` is always present once schemas are registered, but guard
        // anyway so the modifier is a no-op rather than a panic if it is not.
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer",
                SecurityScheme::Http(
                    Http::builder()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            );
        }
    }
}

/// The aggregated `OpenAPI` 3.1 document for the notification REST API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "GridTokenX Notification Service",
        description = "REST API for user notifications and push device-token registration.",
        version = "0.1.1",
    ),
    paths(
        handlers::health_check,
        handlers::list_notifications,
        handlers::mark_notification_as_read,
        handlers::mark_all_notifications_as_read,
        handlers::register_device,
        handlers::revoke_device,
        handlers::list_devices,
    ),
    components(schemas(
        handlers::ListNotificationsResponse,
        handlers::RegisterDeviceRequest,
        handlers::ListDevicesResponse,
        noti_core::domain::Notification,
        noti_core::wire::NotificationView,
        noti_core::domain::NotificationChannel,
        noti_core::domain::NotificationStatus,
        noti_core::domain::DeviceToken,
        noti_core::domain::DevicePlatform,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "notifications", description = "User notification read/list operations"),
        (name = "devices", description = "Push device-token registration"),
        (name = "health", description = "Liveness / readiness probes"),
    )
)]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use super::ApiDoc;
    use utoipa::OpenApi;

    #[test]
    fn spec_serializes_with_paths_schemas_and_security() {
        let doc = ApiDoc::openapi();
        let json = doc.to_json().expect("spec serializes to JSON");

        // Every annotated path is registered.
        for path in [
            "/health",
            "/api/v1/noti",
            "/api/v1/noti/{id}",
            "/api/v1/noti/read-all",
            "/api/v1/noti/devices",
            "/api/v1/noti/devices/{token}",
        ] {
            assert!(doc.paths.paths.contains_key(path), "missing path {path}");
        }

        // Domain + DTO schemas are pulled into components.
        let components = doc.components.expect("components present");
        for schema in [
            "Notification",
            "DeviceToken",
            "ListNotificationsResponse",
            "ListDevicesResponse",
            "RegisterDeviceRequest",
        ] {
            assert!(
                components.schemas.contains_key(schema),
                "missing schema {schema}"
            );
        }

        // Bearer security scheme injected by the modifier.
        assert!(components.security_schemes.contains_key("bearer"));
        assert!(json.contains("bearer"));
    }
}

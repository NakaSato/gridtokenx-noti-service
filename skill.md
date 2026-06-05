---
name: gridtokenx-notification-architecture
description: Guidance for structuring and extending the GridTokenX Notification Service — the trait-based dependency injection pattern, sync-core/async-edges design, Tera template rendering system, multi-channel providers (Email, SMS, Push, Webhook, WebSocket), Kafka event ingestion, RabbitMQ DLX retry flow, and the decoupled WebSocket registry. Use this skill whenever the user asks about how to organize or extend notification handling code; where to add new providers, channels, or event types; how the template system works; where business logic should live vs infrastructure; how retry and idempotency work; or how to maintain the strict dependency layers while adding new delivery mechanisms. Trigger on questions about the noti-service architecture, adding notification features, template rendering, provider implementations, or event-driven notification flows.
---

# GridTokenX Notification Service Architecture

A skill for structuring and extending the centralized notification dispatcher for the GridTokenX platform. The service handles all outbound communications across multiple channels (Email, SMS, Push, Webhook, WebSocket) with HTML templating, idempotency, URL rewriting, and retry logic.

## Core Stance: The Notification Pipeline Is a Pure Function of Events

The service ingests events, renders templates, selects providers, and delivers. Every design decision flows from keeping that pipeline transparent and testable. Three questions worth holding:

1. **Does this belong in the pipeline or in a provider?** Pipeline logic (routing, retry, idempotency) is generic. Provider logic (SMTP handshake, push payload format) is channel-specific. Don't mix them.
2. **Is this sync or async?** Template rendering, variable interpolation, channel mapping — sync. Database writes, queue publishes, SMTP sends — async. The boundary is at the orchestrator's dispatch call.
3. **Does the upstream event provide everything, or must the service fill gaps?** URLs, tokens, and recipient addresses can come from events or be constructed from config. Keep construction at the adapter edge (`consumers.rs`), never in core.

## The Six-Crate Modular Monolith

```
noti-core        → Domain models, 6 DI traits, config, NotiError (thiserror)
noti-protocol    → ConnectRPC/gRPC wire contracts from proto/noti.proto
noti-persistence → SQLx Postgres (dual-pool), Redis cache, RabbitMQ publisher, SMTP, Tera, providers
noti-logic       → NotificationOrchestrator — sync business rules (queue, dispatch, retry)
noti-api         → Axum REST, ConnectRPC service, JWT auth, WebSocket registry
noti-server      → Binary entry, startup wiring, background Kafka + RabbitMQ consumers
```

Dependency flow is strictly downward. `noti-logic` never imports `noti-persistence` or `noti-api`. All cross-layer communication goes through traits defined in `noti-core`.

## The Six DI Traits

Every external dependency is a trait in `noti-core/src/traits.rs`:

| Trait | Methods | Purpose |
|-------|---------|---------|
| `NotificationRepositoryTrait` | 10 (CRUD, idempotency, retry, read-status) | Persist notification lifecycle |
| `NotificationProviderTrait` | 2 (`send`, `provider_id`) | Send via external channel |
| `WebSocketRegistryTrait` | 1 (`send_to_user`) | Track active WS connections |
| `CacheTrait` | 6 (set, get, incr, del, lock, unlock) | Redis cache + distributed locks |
| `TemplateEngineTrait` | 1 sync (`render`) | Render Tera templates |
| `MessageQueueTrait` | 2 (`publish_dispatch`, `publish_retry`) | RabbitMQ task publishing |

Wiring happens in `noti-server/startup.rs` — concrete implementations wrapped in `Arc<dyn Trait>` and injected into `NotificationOrchestrator`.

## The Notification Pipeline

```
Kafka Event → consumers.rs → orchestrator.queue_notification()
                                    ↓
                            idempotency check (Redis)
                            persist notification (Postgres)
                            cache idempotency key (Redis, TTL 3600s)
                            publish dispatch task (RabbitMQ) or fallback spawn
                                    ↓
                    RabbitMQ consumer → orchestrator.dispatch()
                                            ↓
                                    mark Processing
                                    render template (Tera, sync)
                                    select provider by channel
                                    send via provider (async)
                                    update status in DB
```

### Retry Architecture

Failed deliveries are nacked to RabbitMQ with DLX (Dead-Letter Exchange). TTL-based exponential backoff (`delay = 2^retry_count × 60s`), max 5 retries. The retry path: failed send → `increment_retry` in DB → `publish_retry` to RabbitMQ with `expiration` header → TTL expires → DLX routes to `noti.dispatch` → consumer re-attempts → repeat until success or `PermanentFailure`.

### Idempotency

Every notification carries an `idempotency_key` (Kafka events use `kafka:{topic}:{partition}:{offset}`). Before persisting, the orchestrator checks Redis for an existing notification with that key, then caches the new key on success.

## The WebSocket Decoupling Pattern

WebSocket connections live in `noti-api` (Axum route + `DashMap`-based `ConnectionManager`), but the orchestrator needs to push messages without importing the API crate.

Solution:
- `ConnectionManager` in `noti-api` implements `WebSocketRegistryTrait` (defined in `noti-core`)
- `WebSocketProvider` in `noti-persistence` implements `NotificationProviderTrait` but only holds `Arc<dyn WebSocketRegistryTrait>`
- At startup, `ConnectionManager` is wrapped as `Arc<dyn WebSocketRegistryTrait>`, passed to both the Axum router (for upgrades) and `WebSocketProvider` (for sends)

No circular dependencies. The API crate never knows about providers; the persistence crate never knows about Axum.

## Templates

Tera (Jinja2-like) templates in `templates/`. Three formats:
- `.html.tera` — styled HTML email templates (welcome, ERC issued, password reset, email verification)
- `.txt.tera` — plain text for WebSocket notifications and email fallbacks

The `TemplateEngine` loads all templates at startup via glob (`templates/**/*`) with HTML autoescaping enabled. Variables are passed as `serde_json::Value`.

### HTML Email Design

All email HTML templates follow a consistent design system:
- Green gradient header (amber for password reset / security)
- White card with rounded corners and shadow
- Responsive layout, max-width 600px
- CTA button with gradient
- Fallback text link
- Footer with brand

`SmtpProvider` auto-detects HTML content (`<!DOCTYPE` or `<html` prefix) and sends as `multipart/alternative` with both HTML and stripped-text parts.

### URL Rewriting

Email templates contain callback URLs. The `consumers.rs` adapter edge handles three scenarios:

1. **Upstream provides full URL with internal address** → `rewrite_url(frontend_url, upstream_url)` replaces `scheme://host:port` with `FRONTEND_URL`, preserving path and query.
2. **Upstream provides only a token** → `build_callback_url(frontend_url, path, query)` constructs full URL.
3. **No `FRONTEND_URL` configured** → passes through as-is.

## Dual PostgreSQL Pool

`NotificationRepository` routes queries to two pools:
- **High-priority** (full max connections): all writes + idempotency lookups
- **Low-priority** (half max connections): read-only queries (get_by_id, list, count)

## Error Strategy

- `noti-core/error.rs`: `NotiError` enum via `thiserror` — 7 typed variants (`Database`, `Template`, `Provider`, `Validation`, `NotFound`, `Idempotent`, `Internal`)
- All DI traits return `Result<T, NotiError>`
- `noti-api` and `noti-server`: `anyhow` for attaching context at the edges
- Workspace lints: `unsafe_code = "deny"`, `unwrap_used = "deny"`, `pedantic = "warn"`

## How to Extend

### Adding a New Event Type

1. Create template: `templates/<event_name>.html.tera` (email) or `.txt.tera` (WebSocket)
2. Add match arm in `bin/noti-server/src/consumers.rs` → `handle_kafka_message()`
3. Call `orchestrator.queue_notification()` with channel, recipient, template ID, variables, idempotency key
4. If the notification needs a callback URL, use `build_callback_url(frontend_url, path, query)` or `rewrite_url(frontend_url, upstream_url)`

### Adding a New Provider

1. Implement `NotificationProviderTrait` in `crates/noti-persistence/src/providers/`
2. Register in `crates/noti-persistence/src/providers.rs` (module declaration + re-export)
3. Wire into `NotificationOrchestrator::new()` in `bin/noti-server/src/startup.rs`
4. Map the channel in the orchestrator's dispatch logic (`orchestrator.rs` match block)

### Adding a New Delivery Channel

1. Add variant to `NotificationChannel` enum in `noti-core/src/domain.rs`
2. Add provider (see above)
3. Update the orchestrator's channel→provider mapping
4. Add template support if needed

## Dual Server Architecture

The service runs two concurrent servers:
- **HTTP/REST** on `PORT` (default 8080) — Axum with health endpoints, notification CRUD, WebSocket upgrade
- **gRPC + HTTP/3** on `PORT + 10` (default 8090) — ConnectRPC over TCP/HTTP2 and QUIC/HTTP3 via `quinn` + `h3`

Both share the same Axum router. Graceful shutdown via `CancellationToken`.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
cargo build                              # Build entire workspace
cargo check                              # Fast compile check
cargo test                               # Run all tests
cargo test -p noti-logic                 # Test a single crate
cargo test test_queue_notification       # Run a specific test by name
cargo run --package noti-server          # Run the service locally
cargo clippy -- -D warnings             # Lint (clippy::pedantic warn, unwrap_used deny)
```

Database migrations use `sqlx-cli` (`migrations/`, sqlx 0.8 compile-time checked):
```bash
sqlx migrate run --database-url "$DATABASE_URL"
sqlx migrate add <name>
```

Smoke-test running service against live HTTP/gRPC endpoints:
```bash
./scripts/test_endpoints.sh              # curl health, CRUD, WebSocket upgrade
```

> Deeper references: [ARCHITECTURE.md](ARCHITECTURE.md) (layered diagrams, hexagonal ports/adapters), [README.md](README.md) (service role, channel list), `noti-service-design.md` (design rationale).

## Architecture

Modular monolith Cargo workspace, **hexagonal (ports & adapters)**. Edition 2024. Six crates with strict downward (acyclic) dependency flow:

```
bin/noti-server → noti-api → noti-logic → noti-core
                → noti-persistence       → noti-core
                              noti-protocol → (prost/tonic)
```

| Crate | Role |
|---|---|
| **noti-core** | Domain models (`Notification`, `NotificationChannel`, `NotificationStatus`), DI trait contracts, config, typed errors (`NotiError` via thiserror) |
| **noti-protocol** | ConnectRPC/gRPC wire contracts generated from `proto/noti.proto` |
| **noti-persistence** | Infrastructure adapters: SQLx Postgres repo, Redis cache, RabbitMQ publisher, Kafka consumer setup, SMTP (lettre), Tera template engine, mock providers |
| **noti-logic** | `NotificationOrchestrator` — synchronous business logic for queue/dispatch/retry, channel→provider mapping |
| **noti-api** | Axum REST handlers, ConnectRPC service, JWT auth middleware, WebSocket connection registry (`DashMap`-based) |
| **noti-server** | Binary entry point. Wires all dependencies via trait objects in `startup::run()`. Hosts background consumers (Kafka + RabbitMQ) |

### Key Design Patterns

- **Trait-based DI**: All traits defined in `noti-core/traits.rs` (`NotificationRepositoryTrait`, `CacheTrait`, `MessageQueueTrait`, `NotificationProviderTrait`, `TemplateEngineTrait`, `WebSocketRegistryTrait`). Concrete implementations in `noti-persistence`. Wired in `noti-server/startup.rs` with `Arc<dyn Trait>`.
- **Sync core, async edges**: `noti-logic` is synchronous. All async I/O happens at the adapter layer.
- **Hybrid error strategy**: `NotiError` (thiserror) at core boundaries, `anyhow` at API/server edges.
- **Decoupled WebSocket**: `ConnectionManager` in `noti-api` implements `WebSocketRegistryTrait`. `WebSocketProvider` in `noti-persistence` holds `Arc<dyn WebSocketRegistryTrait>` — no circular dependency.

### Notification Flow

1. **Ingestion**: Kafka consumer (`consumers.rs`) listens to `iam.user.events`, `iam.audit.events` (and others). Maps event types (`UserRegistered`, `OrderMatched`, `ErcIssued`, `PasswordResetRequested`, `VerificationEmailRequested`, `UserOnboarded`, `MeterOnboarded`, `UserWalletLinked`) → `orchestrator.queue_notification()`.
2. **Queue**: Orchestrator checks idempotency (Redis), persists notification (Postgres), publishes dispatch task (RabbitMQ `noti.dispatch`).
3. **Dispatch**: RabbitMQ consumer picks up task → `orchestrator.dispatch()` renders template (Tera), selects provider by channel, sends.
4. **Retry**: Failed deliveries nacked to RabbitMQ DLX with TTL-based exponential backoff (max 5 retries).
5. **Real-time**: WebSocket channel pushes notifications immediately to connected users via `ConnectionManager`.

### Dual Server

The service runs two servers concurrently:
- **HTTP/REST** on `PORT` (default 8080) — Axum with health, notification CRUD, WebSocket upgrade
- **gRPC + HTTP/3** on `PORT + 10` (default 8090) — ConnectRPC over TCP/HTTP2 and QUIC/HTTP3 via `quinn` + `h3`

## Configuration

Environment-driven (loaded via `dotenvy` + `config` crate). Key variables:

| Variable | Default | Notes |
|---|---|---|
| `PORT` | `8080` | HTTP port; gRPC = PORT + 10 |
| `DATABASE_URL` | required | PostgreSQL |
| `REDIS_URL` | required | Cache, idempotency, rate-limiting |
| `KAFKA_BROKERS` | optional | Comma-separated |
| `RABBITMQ_URL` | optional | Retry/delivery queue |
| `JWT_SECRET` | required | WebSocket auth |
| `SMTP_HOST` | optional | If empty → mock email provider |
| `SMTP_TLS_MODE` | `starttls` | `starttls`, `tls`, or `none` |
| `CERT_FILE` / `KEY_FILE` | `infra/certs/*` | TLS certs for HTTP/3 QUIC |

## Lint Rules

Workspace-level lints in root `Cargo.toml`:
- `unsafe_code = "deny"`
- `clippy::pedantic = "warn"` (priority -1)
- `clippy::unwrap_used = "deny"` — use `?` with `.context()` or `.expect("reason")` only in init code

## Templates

Tera templates in `templates/` directory. Naming: `<template_name>.txt.tera` (plain text) or `.html.tera` (rich email). The `TemplateEngine` in `noti-persistence/templating.rs` loads them at startup.

## Proto

Single proto file at `proto/noti.proto`. Code generation via `buffa-build` + `connectrpc-build` in `noti-protocol/build.rs`. Generated code in `noti-protocol/src/noti.rs`.

## Adding a New Event Type

1. Add template in `templates/<event_name>.txt.tera`
2. Add a match arm in `bin/noti-server/src/consumers.rs` → `handle_kafka_message()`
3. Call `orchestrator.queue_notification()` with appropriate channel, recipient, template, variables, and idempotency key

## Adding a New Provider

1. Implement `NotificationProviderTrait` in `crates/noti-persistence/src/providers/`
2. Register in `crates/noti-persistence/src/providers/mod.rs`
3. Wire into the orchestrator in `bin/noti-server/src/startup.rs`
4. Map the channel in `noti-logic/src/orchestrator.rs` dispatch logic

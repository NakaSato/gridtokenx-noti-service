# gridtokenx-noti-service — Rust Project Structure

**4-layer hexagonal (ports & adapters) Cargo workspace.** Edition 2024. Strict downward (acyclic) dependency flow. One of the two reference implementations (with IAM) for the project-wide 4-layer pattern.

> Verified against source. Where the project-wide aspirational design (NATS taxonomy, HTTP/3, `buf`, cargo-deny) differs from what is actually wired today, this doc states the **current reality** and flags the gap.

---

## 1. Crate Layout (actual)

```
gridtokenx-noti-service/
├── Cargo.toml                       # virtual workspace; [workspace.dependencies] single source of truth
├── Cargo.lock                       # committed
├── README.md
├── CLAUDE.md                        # service-specific invariants
├── ARCHITECTURE.md                  # layered diagrams, hexagonal ports/adapters
├── SKILL.md                         # subsystem expert knowledge
├── migrations/                      # sqlx migrations (compile-time checked, sqlx 0.8)
├── proto/                           # noti.proto (single file; no buf.yaml)
├── templates/                       # Tera templates: <name>.txt.tera / .html.tera
├── crates/
│   ├── noti-core/                   # ① DOMAIN: domain.rs, traits.rs, error.rs, config.rs. NO tokio/sqlx/transport
│   ├── noti-persistence/            # ② ADAPTERS: repository, cache, templating, messaging/, providers/
│   ├── noti-logic/                  # ③ ORCHESTRATION: orchestrator/{mod,queue,dispatch,query}.rs
│   ├── noti-protocol/               # generated proto code (build.rs → OUT_DIR/_noti_include.rs)
│   └── noti-api/                    # ④ TRANSPORT: handlers, grpc, websocket, auth
└── bin/
    └── noti-server/                 # binary: main, startup, telemetry, consumers/{mod,events,url}.rs
```

**Not present** (despite project-wide template): `rust-toolchain.toml`, `deny.toml` (cargo-deny not configured), `proto/buf.yaml` (codegen uses `connectrpc_build` directly), top-level `tests/` dir (tests are inline `#[cfg(test)]`).

Transport is split: `noti-api` (crate — handlers/services) + `bin/noti-server` (binary — wires `Arc<dyn Trait>` deps in `startup::run()`, hosts Kafka + RabbitMQ consumers).

---

## 2. Layer Responsibilities (the contract)

| Crate | Owns | Forbidden imports | Key deps |
|---|---|---|---|
| **noti-core** | `Notification`, `NotificationChannel`, `NotificationStatus`, DI traits, `Config`, `NotiError` (thiserror) | `tokio`, `sqlx`, `redis`, `rdkafka`, `lapin`, `axum`, `connectrpc`, `tonic` | `serde`, `chrono`, `uuid`, `thiserror`, `async-trait` |
| **noti-persistence** | Trait *impls*: `repository.rs` (Sqlx repo), `cache.rs` (Redis), `messaging/{kafka,rabbitmq}.rs`, `providers/{smtp,websocket}.rs`, `providers.rs` (mocks), `templating.rs` (Tera). | `axum`, `connectrpc`, non-I/O business logic | `noti-core`, `sqlx`, `redis`, `rdkafka`, `lapin`, `lettre`, `tera` |
| **noti-logic** | `NotificationOrchestrator` — queue/dispatch/retry, channel→provider mapping | `axum`, `tonic::transport`, raw connectrpc handlers | `noti-core` (+`mocks` in dev), `noti-persistence`, `tokio`, `tracing` |
| **noti-api** | Axum REST handlers, ConnectRPC service (`grpc.rs`), JWT auth (`auth.rs`), WebSocket `ConnectionManager` (`DashMap`) | Business logic (delegate to `-logic`) | `noti-core`, `noti-logic`, `noti-protocol`, `axum`, `connectrpc`, `tower-http` |
| **noti-protocol** | `build.rs` codegen for `noti.proto`; re-exports generated types. Pure DTO crate. | Everything else | `buffa`/`prost`, `connectrpc-build`, `tonic` |

### 2.1 Dependency-inversion rule

```
noti-api  ─────────┐
       │           ▼
       └──► noti-logic ──► noti-persistence
                  │                │
                  └──► noti-core ◄─┘
```

Dependencies point *inward* toward `noti-core`. New gRPC endpoint in `-api` does not force a `-core` change. Swap the persistence backend without touching `-logic`. `noti-core` depends on nothing from the other layers.

### 2.2 `mocks` feature pattern

`noti-core/Cargo.toml`:
```toml
[features]
mocks = ["mockall"]
```

`noti-logic` consumes mocks (dev-dependency) for unit tests:
```toml
noti-core = { workspace = true, features = ["mocks"] }
```

### 2.3 Sync core, async edges

`noti-logic` orchestrator methods are the business core. Async I/O lives in the adapter (`-persistence`) and transport (`-api`) layers. Note: `noti-logic` *does* depend on `tokio` — it uses `tokio::spawn` for the in-process dispatch/retry fallback when the message queue is unavailable.

---

## 3. Notification Flow

1. **Ingestion**: Kafka consumer (`bin/noti-server/src/consumers/`) subscribes to topics `kafka_topic_user_events` (default `iam.user.events`) and `kafka_topic_audit_events` (default `iam.audit.events`). `consumers/mod.rs::handle_kafka_message` decodes, then `events.rs::route` matches `event_type` → typed handler. **13 routed event types**: `UserRegistered`, `OrderMatched`, `SettlementProcessed`, `ErcIssued`, `VppDispatched`, `PasswordResetRequested`, `VerificationEmailRequested`, `UserOnboarded`, `MeterOnboarded`, `UserWalletLinked` (+ inline arms). Each → `orchestrator.queue_notification()`.
2. **Queue** (`orchestrator/queue.rs`): dual-layer idempotency — repo `create()` returns the existing row on `idempotency_key` conflict (source of truth), *then* caches `idempotency:{key}` in Redis 3600s (best-effort). Persist (Postgres) → `mq.publish_dispatch(id)` (RabbitMQ `noti.dispatch`). If MQ fails / absent → `tokio::spawn` in-process dispatch.
3. **Dispatch** (`orchestrator/dispatch.rs`): RabbitMQ consumer → `orchestrator.dispatch()` renders Tera template, selects provider by channel, sends.
4. **Retry**: on failure, `MAX_RETRIES = 5`, backoff `delay_ms = 2^retry_count * 60 * 1000` (exponential, minutes); status reset to `Pending`, scheduled via RabbitMQ DLX TTL (`publish_retry`), or in-process `spawn_in_process_retry` fallback. At/over max → `PermanentFailure`.
5. **Real-time**: WebSocket pushes to connected users via `ConnectionManager`.

### Decoupled WebSocket (no circular dep)

`ConnectionManager` in `noti-api` implements `WebSocketRegistryTrait`. `WebSocketProvider` in `noti-persistence` holds `Arc<dyn WebSocketRegistryTrait>` — no cycle.

---

## 4. Dual Server

Two servers run concurrently (two tokio tasks, graceful shutdown via cancellation token):
- **HTTP/REST** on `port` (default 8080) — Axum: health, notification CRUD, WebSocket upgrade.
- **gRPC** on `grpc_port` (default `port + 10` = 8090) — ConnectRPC over plain TCP/HTTP2 (`axum::serve`).

> `quinn` / `h3` / `h3-quinn` are declared in `bin/noti-server/Cargo.toml` but **not yet wired** — HTTP/3 over QUIC is planned, not active. No QUIC listener or cert loading exists in source today.

---

## 5. Workspace `Cargo.toml` Stack (actual versions)

```toml
[workspace]
resolver = "2"
members = ["crates/noti-core", "crates/noti-protocol", "crates/noti-persistence",
           "crates/noti-logic", "crates/noti-api", "bin/noti-server"]

[workspace.package]
version = "0.1.1"
edition = "2024"

[workspace.lints.rust]
unsafe_code = "deny"

[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
unwrap_used = "deny"

[workspace.dependencies]
# Internal + shared (path deps)
noti-core = { path = "crates/noti-core" }            # + noti-protocol/persistence/logic/api
gridtokenx-blockchain-core = { path = "../gridtokenx-blockchain-core" }
gridtokenx-telemetry       = { path = "../gridtokenx-telemetry" }

# Async runtime
tokio = { version = "1.48", features = ["rt-multi-thread","macros","io-util","time","sync","signal","parking_lot"] }
async-trait = "0.1"

# Web / transport
axum = { version = "0.8.7", features = ["macros"] }
tower-http = { version = "0.6.1", features = ["full"] }
connectrpc = { version = "0.6.1", features = ["axum","server","client"] }
tonic = "0.12.3"
prost = "0.13"
buffa = "0.6.0"

# HTTP/3 (declared, not yet wired)
quinn = { version = "0.11", features = ["rustls"] }
h3 = "0.0.8"
h3-quinn = "0.0.10"
rustls = { version = "0.23", features = ["ring"] }

# Persistence
sqlx = { version = "0.8", features = ["macros","postgres","runtime-tokio-rustls","chrono","uuid","rust_decimal","migrate"] }
redis = { version = "0.32", features = ["tokio-comp","connection-manager"] }
rdkafka = { version = "0.37", features = ["zstd"] }       # Kafka consumer
lapin = "2.5"                                             # RabbitMQ DLX

# Templating, mail, auth
tera = "1.20"
lettre = { version = "0.11", default-features = false, features = ["tokio1-rustls-tls","serde","smtp-transport","builder"] }
jsonwebtoken = "9.3"

# Telemetry / metrics
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter","json"] }
metrics = "0.24"
metrics-exporter-prometheus = "0.17"

# Errors, IDs, serde, config, concurrency, test
anyhow = "1.0"; thiserror = "2.0"
uuid = { version = "1.0", features = ["v4","v5","serde"] }
chrono = { version = "0.4", features = ["serde"] }
serde = { version = "1.0", features = ["derive"] }; serde_json = "1.0"
config = "0.15.19"; dotenvy = "0.15"
dashmap = "6.1"          # WebSocket connection registry
mockall = "0.13"

[profile.release]
opt-level = 3; lto = true; codegen-units = 1; panic = "abort"; strip = true
```

---

## 6. Configuration (from `noti-core/config.rs`)

Loaded via `config` + `dotenvy`, `RUN_MODE`-driven (`config/default`, `config/{run_mode}`, then env).

| Variable | Required? | Default | Notes |
|---|---|---|---|
| `PORT` | no | `8080` | HTTP port |
| `GRPC_PORT` | no | `port + 10` | ConnectRPC server |
| `DATABASE_URL` | **yes** | — | PostgreSQL |
| `KAFKA_BROKERS` | **yes** | — | Comma-separated |
| `RABBITMQ_URL` | **yes** | — | Retry/delivery queue |
| `REDIS_URL` | **yes** | — | Cache, idempotency, rate-limiting |
| `JWT_SECRET` | **yes** | — | WebSocket / API auth |
| `KAFKA_TOPIC_USER_EVENTS` | no | `iam.user.events` | |
| `KAFKA_TOPIC_AUDIT_EVENTS` | no | `iam.audit.events` | |
| `DATABASE_MAX_CONNECTIONS` | no | `20` | also min(5)/acquire(30s)/idle(600s) |
| `SMTP_HOST` | no | — | If unset → `MockEmailProvider` |
| `SMTP_PORT/USER/PASS/FROM` | no | — | |
| `SMTP_TLS_MODE` | no | `starttls` | `starttls` \| `tls` (465) \| `none` (Mailpit) |
| `FRONTEND_URL` | no | — | Email callback link base |
| `TWILIO_*`, `FCM_PROJECT_ID` | no | — | Reserved; SMS/Push are mocks today |

> No `CERT_FILE`/`KEY_FILE` in config — TLS cert loading not implemented (ties to the dormant HTTP/3 path).

---

## 7. Messaging & Contracts (noti's actual wiring)

**Inbound** — Kafka topics (not NATS): `iam.user.events`, `iam.audit.events`.
**Dispatch/retry** — RabbitMQ (`noti.dispatch` + DLX), `lapin`.
**gRPC** — ConnectRPC service `NotificationService` from `proto/noti.proto` (package `noti`, *not* `noti.v1`).

> The project-wide `gtx.<service>.<entity>.<event>.vN` **NATS** taxonomy does not apply here — noti-service consumes Kafka and dispatches over RabbitMQ. No NATS client is wired.

### Saga ownership

| Saga | Orchestrator | Compensation |
|---|---|---|
| Notification dispatch | `noti-logic::NotificationOrchestrator` | RabbitMQ DLX + retry, `MAX_RETRIES = 5` |

Queue path persists to Postgres before publishing the RabbitMQ task (transactional-outbox shape); idempotency enforced repo-side on `idempotency_key`.

### Proto codegen

`noti-protocol/build.rs` runs `connectrpc_build::Config` over `../../proto/noti.proto` → emits `OUT_DIR/_noti_include.rs`, included by `noti-protocol/src/lib.rs` as `pub mod noti`. Generated types map to `noti-core` domain types in `-api` handlers (no prost DTO leak into `-logic`).

---

## 8. Adding Things

**New event type:**
1. Add template `templates/<event_name>.txt.tera` (and `.html.tera` for rich email).
2. Add typed payload struct + handler fn in `bin/noti-server/src/consumers/events.rs`, and a match arm in `route()`.
3. Handler calls `orchestrator.queue_notification(user_id, channel, recipient, template_id, variables, idempotency_key)`.

**New provider:**
1. Implement `NotificationProviderTrait` in `crates/noti-persistence/src/providers/`.
2. Export from `crates/noti-persistence/src/providers.rs`.
3. Add the provider field to `NotificationOrchestrator` in `noti-logic/src/orchestrator/mod.rs` and select it by channel in `orchestrator/dispatch.rs`.
4. Wire the concrete impl in `bin/noti-server/src/startup.rs`.

> SMS / Push / Webhook are currently `MockSmsProvider` / `MockPushProvider` / `MockWebhookProvider` — not yet implemented. Email uses `SmtpProvider` when `SMTP_HOST` is set, else `MockEmailProvider`.

---

## 9. Anti-Patterns to Avoid

1. **Async in `noti-core`** — keep the domain crate free of `tokio`/`sqlx`/transport. Use `parking_lot` for sync mutability, or push state to `-persistence`.
2. **Proto types leaking into `-logic`** — map generated DTOs → `noti-core` types in the `-api` handler.
3. **Business logic in handlers** — handlers validate input, call `-logic`, return response.
4. **`unwrap()` in production paths** — `unwrap_used = "deny"`. Note `panic = "abort"` in release: a panic kills the process (no unwind). Use `?` + `.context()`; `.expect("reason")` only in fatal init.
5. **Direct DB calls from handlers** — always through the repository trait.
6. **Dropping repo-side idempotency** — Redis cache is an optimization; the unique-key conflict in `repo.create()` is the source of truth. Keep both.

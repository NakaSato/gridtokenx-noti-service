# SKILL.md — noti-service subsystem expert knowledge

Operational deep-knowledge for working *inside* `gridtokenx-noti-service`. Reflects actual code, not aspirational design. For build commands see `CLAUDE.md`; for layer diagrams see `ARCHITECTURE.md`; for crate-tree rationale see `gridtokenx-rust-structure.md`.

---

## Crate map (actual)

```
crates/noti-core         domain.rs, traits.rs, error.rs, config.rs   — sync, no I/O deps
crates/noti-protocol     build.rs (buffa-build + connectrpc-build) → generated noti.v1
crates/noti-persistence  repository.rs, cache.rs, templating.rs,
                         messaging/{kafka,rabbitmq}.rs,
                         providers/{smtp,websocket}.rs, providers.rs (mocks)
crates/noti-logic        orchestrator/{mod,queue,dispatch,query}.rs  — sync core
crates/noti-api          handlers.rs, grpc.rs, websocket.rs, auth.rs
bin/noti-server          main.rs, startup.rs, telemetry.rs,
                         consumers/{mod,events,url}.rs               — wiring + consumers
```

Workspace: edition **2024**, version 0.1.1. `release` profile = `lto=true`, `codegen-units=1`, `panic=abort`, `strip`. Two external path deps: `../gridtokenx-blockchain-core`, `../gridtokenx-telemetry`.

---

## Domain types (noti-core/domain.rs)

- `NotificationChannel` = `Email | Sms | Push | Webhook | WebSocket`
- `NotificationStatus` = `Pending | Processing | Sent | Delivered | Failed | PermanentFailure`
- `Notification` struct — note `user_id: Option<Uuid>` (system notifications have none), `idempotency_key: Option<String>`, `retry_count: i32`, `next_retry_at`, `provider_id`/`provider_ref`, `read_at`.

## DI traits (noti-core/traits.rs)

All `#[cfg_attr(feature = "mocks", mockall::automock)]`. All async except `TemplateEngineTrait` (sync — pure string transform).

| Trait | Impl (persistence) | Notes |
|---|---|---|
| `NotificationRepositoryTrait` | `repository.rs` SqlxNotiRepo | `create` is idempotent: returns existing row on `idempotency_key` conflict |
| `NotificationProviderTrait` | `providers/smtp.rs`, `providers.rs` mocks, `providers/websocket.rs` | `send(recipient, content) -> provider_ref`; `provider_id()` for audit |
| `WebSocketRegistryTrait` | `noti-api::websocket::ConnectionManager` | implemented in **api**, not persistence (avoids cycle) |
| `CacheTrait` | `cache.rs` Redis | includes `lock`/`unlock` (distributed lock), `increment_with_ttl` (rate-limit) |
| `TemplateEngineTrait` | `templating.rs` Tera | **sync** |
| `MessageQueueTrait` | `messaging/rabbitmq.rs` | `publish_dispatch`, `publish_retry(id, delay_ms)` |

---

## Orchestrator flow (noti-logic) — the critical path

`NotificationOrchestrator` holds 5 named providers as separate fields (`email/sms/push/webhook/websocket`), plus repo, template_engine, cache, and `mq: Option<...>`. Methods split across `impl` blocks in `queue.rs` / `dispatch.rs` / `query.rs`.

### queue_notification (queue.rs)
1. Build `Notification` (status `Pending`), `repo.create()`.
2. **Idempotency = dual-layer**: repo returns existing row on key conflict → if `saved.id != requested_id`, return early (duplicate). *Then* cache `idempotency:{key}` in Redis 3600s (best-effort, warns on fail).
3. Trigger dispatch: `mq.publish_dispatch(id)`. **If MQ fails or `mq` is None → fallback `tokio::spawn(orchestrator.dispatch(id))` in-process.**

### dispatch (dispatch.rs)
1. `repo.get_by_id`; skip unless status `Pending`/`Processing` (re-delivery guard).
2. Mark `Processing`, render template, select provider by channel, `send()`.
3. **Success** → `update_status(Sent, provider_ref)`.
4. **Failure** → retry logic:
   - `const MAX_RETRIES: i32 = 5`. At/over → `PermanentFailure`, propagate err.
   - Else: `delay_ms = 2^retry_count * 60 * 1000` (exponential, minutes). `increment_retry` + status back to `Pending`. Schedule via `mq.publish_retry`; on MQ failure fall back to `spawn_in_process_retry` (tokio sleep + redispatch).

> Retry is driven by RabbitMQ DLX TTL in prod; in-process spawn is the degraded fallback when MQ absent/erroring. Both honor the same backoff.

---

## Ingestion: Kafka consumers (bin/noti-server/consumers)

`consumers/mod.rs::handle_kafka_message` decodes, then `events.rs::route` matches `event_type` string → typed handler. **13 match arms** (events.rs:177):

`UserRegistered, OrderMatched, SettlementProcessed, ErcIssued, VppDispatched, PasswordResetRequested, VerificationEmailRequested, UserOnboarded, MeterOnboarded, UserWalletLinked` (Settlement/PasswordReset/VerificationEmail have inline arms).

Each payload is a typed `#[derive(Deserialize)]` struct (e.g. `UserRegistered{email,username}`) — missing/renamed field fails at deserialize boundary, not silently. Idempotency key derived from `MsgCtx{topic,partition,offset}`. `consumers/url.rs` builds callback URLs (`build_callback_url`, `rewrite_url`) using `frontend_url`.

**Add event type**: typed struct + handler fn in `events.rs`, match arm in `route`, template in `templates/<name>.txt.tera`. See CLAUDE.md.

---

## Wiring (bin/noti-server/startup.rs)

- **Two Postgres pools**: `high_priority_pool` + `low_priority_pool` (separate `PgPoolOptions`). Check which the repo uses for reads vs writes before changing pool sizing.
- **Email provider fallback**: `if config.smtp_host.is_some()` → `SmtpProvider::new(...)` else `MockEmailProvider` (warns). **Webhook = real** (`WebhookProvider`, HTTP POST + SSRF guard). SMS/Push still mocks (`MockSmsProvider`/`MockPushProvider` — local capture sink, not real vendors).
- **RabbitMQ is required**: empty `rabbitmq_url` → `anyhow::bail!` at startup (durable retry mandatory). No more silent in-memory-only mode.
- WebSocket: `ConnectionManager::new()` (api) wrapped by `WebSocketProvider` (persistence) holding `Arc<dyn WebSocketRegistryTrait>`.
- gRPC: `NotificationGrpcService` registered on connectrpc router → `into_axum_router()`.
- **Dual server, two tokio tasks**: HTTP on `config.port` (`0.0.0.0`), gRPC on `grpc_port = config.grpc_port.unwrap_or(port + 10)`. Both graceful-shutdown via cancellation token.
- Kafka + RabbitMQ consumers spawned as background tasks (guarded by config presence).

---

## Gotchas

1. **`unwrap_used = "deny"`, `unsafe_code = "deny"`, `clippy::pedantic = warn`** workspace-wide. Use `?` + context; `.expect("reason")` only in fatal init.
2. **`panic = "abort"` in release** — no unwinding; a panic kills the process. Don't rely on `catch_unwind`.
3. **TemplateEngineTrait is sync** — don't `.await` it. Tera loads templates at startup from `templates/`; a missing template is a render-time `NotiError`, surfaces as dispatch failure → retry.
4. **Idempotency lives in the repo, not just cache** — the unique-key conflict in `create()` is the source of truth; Redis cache is an optimization. Don't remove the repo-side dedup.
5. **RabbitMQ required at boot** — startup `bail!`s on empty `rabbitmq_url`. The in-process dispatch/retry path still exists as a degraded fallback *when a live MQ errors mid-run*, but you can no longer start with no broker (retries would be lost on restart).
6. **Webhook SSRF guard** (`providers/webhook.rs`): http/https only, redirects disabled, resolved IP must be public unicast (blocks loopback/RFC1918/link-local/CGNAT/ULA + v4-mapped), connection pinned to vetted addr (anti-rebind). Reaching an internal URL → `NotiError::Internal` → counts as dispatch failure → retry.
7. **System notifications** have `user_id: None` — WebSocket push and user-list queries must handle that.
8. **gRPC stack** = connectrpc 0.6 + buffa 0.6 (not raw tonic services). Codegen in `noti-protocol/build.rs`. Regenerate by touching `proto/noti.proto` + rebuild.

---

## Common tasks → entry points

| Task | Files |
|---|---|
| New event → notification | `consumers/events.rs`, `templates/<name>.*.tera` |
| New delivery provider | `noti-persistence/providers/`, `providers.rs`, orchestrator field in `orchestrator/mod.rs`, wire in `startup.rs` |
| Change retry policy | `orchestrator/dispatch.rs` (`MAX_RETRIES`, `delay_ms` formula) |
| Repo query / schema | `noti-persistence/repository.rs`, `migrations/` |
| WebSocket behavior | `noti-api/websocket.rs` (`ConnectionManager`), `providers/websocket.rs` |
| Auth / JWT | `noti-api/auth.rs` (`JWT_SECRET`) |
| Template rendering | `noti-persistence/templating.rs` |

## Tests

Unit tests inline `#[cfg(test)]`. `noti-logic` orchestrator tests hand-roll mock impls (see `orchestrator/mod.rs` tests: `test_queue_notification`, `test_queue_notification_deduplicates_idempotency_key`) — also available via `noti-core` `mocks` feature (`mockall`). Run one: `cargo test -p noti-logic test_queue_notification`. Endpoint smoke: `./scripts/test_endpoints.sh`.

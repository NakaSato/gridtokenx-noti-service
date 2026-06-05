# Design Spec: gridtokenx-noti-service

**Date:** 2026-05-24 (updated 2026-05-31)
**Status:** Implemented
**Topic:** Notification Service (Email, SMS, Webhook, Push, WebSocket)

---

## 1. Executive Summary

The `gridtokenx-noti-service` is a centralized, stateful notification dispatcher for the GridTokenX ecosystem. It provides a unified REST/ConnectRPC API and event-driven consumer loops for sending Email (SMTP with multipart HTML+text), SMS, Webhooks, Push notifications, and real-time WebSocket alerts. Every notification is persisted in PostgreSQL with a full audit trail, idempotency protection via Redis, and exponential backoff retries via RabbitMQ DLX.

---

## 2. Goals & Success Criteria

- **Multi-channel support:** Email (SMTP/lettre with HTML), SMS (Mock), Webhooks (Mock), Push (Mock), WebSocket (real-time).
- **HTML email:** Styled HTML templates for all email channels with automatic multipart generation (HTML + plain text fallback).
- **Auditability:** Every notification request, attempt, and final status persisted in PostgreSQL.
- **Resilience:** Exponential backoff retries via RabbitMQ DLX (2^n × 60s, max 5 retries).
- **Idempotency:** Prevent duplicate notifications via unique idempotency keys checked in Redis.
- **URL rewriting:** Email callback links automatically rewritten from internal addresses to the configured frontend URL.
- **Observability:** Prometheus metrics + structured JSON logging via `tracing`.

---

## 3. Architecture

The service follows hexagonal (ports-and-adapters) architecture with "Sync Core, Async Edges" as a 6-crate modular monolith.

### 3.1 Components

- **Inbound Adapters:**
    - `NotificationGrpcService`: ConnectRPC/gRPC for synchronous `SendNotification` and `GetNotificationStatus` RPCs.
    - `KafkaConsumer`: Subscribes to `iam.user.events` and `iam.audit.events`.
    - `RabbitMQConsumer`: Processes the `noti.dispatch` queue for delivery tasks.
- **Core Orchestrator:**
    - `NotificationOrchestrator`: Validates requests, persists `Pending` state, renders templates, selects providers, handles retry logic.
    - `TemplateEngine`: Tera-based template rendering (sync, with HTML autoescaping).
- **Provider Adapters:**
    - `SmtpProvider`: `lettre` SMTP with auto HTML detection → multipart/alternative or plain text.
    - `WebSocketProvider`: Routes real-time alerts through decoupled `ConnectionManager`.
    - `MockProvider` (SMS, Push, Webhooks, Email fallback): Logs to console.

### 3.2 Caching (Redis)

Redis provides:
- **Idempotency cache:** Key-value with TTL (3600s) to prevent duplicate processing.
- **Distributed locks:** `SET NX EX` pattern for concurrency control.
- **Rate limiting:** Atomic `INCR + EXPIRE` pipeline.

### 3.3 Dual PostgreSQL Pool

`NotificationRepository` uses two `PgPool` instances:

| Pool | Connections | Used for |
|:---|:---|:---|
| **High-priority** | `database_max_connections` | Writes: create, update_status, increment_retry, mark_as_read, idempotency lookup, pending-for-retry |
| **Low-priority** | `database_max_connections / 2` | Reads: get_by_id, list_by_user, get_unread_count |

### 3.4 Queuing Strategy

- **Kafka (External Event Source):** Consumes `iam.user.events` and `iam.audit.events`. Maps event types to notification templates and channels.
- **RabbitMQ (Internal Tasks):**
    - `noti.exchange`: Main Direct exchange.
    - `noti.dispatch`: Primary dispatch queue.
    - `noti.retry`: Delay queue with `x-dead-letter-exchange = noti.exchange` and `x-dead-letter-routing-key = noti.dispatch`. Messages include `expiration` header for TTL-based delay.

### 3.5 Data Model (PostgreSQL)

```sql
CREATE TYPE notification_channel AS ENUM ('email', 'sms', 'push', 'webhook', 'websocket');

CREATE TYPE notification_status AS ENUM (
    'pending', 'processing', 'sent', 'delivered', 'failed', 'permanent_failure'
);

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID,                                        -- Optional (null for anonymous emails)
    channel notification_channel NOT NULL,
    status notification_status NOT NULL DEFAULT 'pending',
    recipient TEXT NOT NULL,                              -- Email, phone, wallet address, etc.
    template_id TEXT NOT NULL,                            -- e.g. "welcome.html.tera"
    variables JSONB DEFAULT '{}',                         -- Template context variables
    provider_id TEXT,                                     -- e.g. 'smtp', 'mock-sms'
    provider_ref TEXT,                                    -- External provider message ID
    retry_count INT DEFAULT 0,
    next_retry_at TIMESTAMPTZ DEFAULT NOW(),
    error_message TEXT,
    idempotency_key TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,                                  -- Set when status = 'sent'
    read_at TIMESTAMPTZ                                   -- NULL until user reads
);

CREATE INDEX idx_notifications_status_retry ON notifications(status, next_retry_at) WHERE status = 'pending';
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
CREATE INDEX idx_notifications_user_unread ON notifications(user_id) WHERE read_at IS NULL;
```

---

## 4. Workflows

### 4.1 Synchronous Request (gRPC/ConnectRPC)

1. Client sends `SendNotification` RPC with channel, recipient, template_id, variables, and optional idempotency_key.
2. Orchestrator checks `idempotency_key` in Redis.
3. If new, persists record as `Pending` in PostgreSQL.
4. Publishes `NotificationTask` to RabbitMQ `noti.dispatch` (or spawns in-process fallback).
5. Returns `NotificationResponse { notification_id, status: "accepted" }` immediately.

### 4.2 Event-Driven Request (Kafka)

1. `KafkaConsumer` receives event from `iam.user.events` or `iam.audit.events`.
2. Event type is mapped to channel + template + variables:

| Event Type | Channel | Template | Notes |
|:---|:---|:---|:---|
| `UserRegistered` | Email | `welcome.html.tera` | Styled welcome with feature list |
| `OrderMatched` | WebSocket | `trade_matched.txt.tera` | Sent to both buyer and seller |
| `ErcIssued` | Email | `erc_issued.html.tera` | Certificate card with amount |
| `PasswordResetRequested` | Email | `password_reset.html.tera` | URL rewritten via `FRONTEND_URL` |
| `VerificationEmailRequested` | Email | `verify_email.html.tera` | URL rewritten via `FRONTEND_URL` |
| `UserOnboarded` | WebSocket | `user_onboarded.txt.tera` | PDA + transaction signature |
| `MeterOnboarded` | WebSocket | `meter_onboarded.txt.tera` | Meter ID + type + tx signature |
| `UserWalletLinked` | WebSocket | `security_alert.txt.tera` | Security alert with wallet address |

3. Idempotency key derived from Kafka topic + partition + offset.
4. URL rewriting: `consumers.rs` replaces the scheme+host+port of upstream URLs with `FRONTEND_URL` while preserving path and query string. If upstream provides only a token, constructs full URL from config.

### 4.3 Dispatch (RabbitMQ Consumer)

1. Consumer picks up `NotificationTask` from `noti.dispatch`.
2. `orchestrator.dispatch(id)`:
   - Marks status as `Processing`.
   - Renders template via `TemplateEngineTrait::render()` (sync).
   - Selects provider by `NotificationChannel`.
   - `SmtpProvider` auto-detects HTML content → sends as `multipart/alternative` (HTML + stripped text) or plain text.
   - On success: marks `Sent` + sets `sent_at = NOW()`.
   - On failure with retries remaining: increments `retry_count`, calculates delay (`2^n × 60s`), publishes to `noti.retry` with TTL.
   - On failure with max retries exceeded: marks `PermanentFailure`.

### 4.4 Retry Path

```
Failed delivery
  → increment_retry_count in DB
  → publish_retry to RabbitMQ with expiration TTL
  → TTL expires → DLX routes to noti.dispatch
  → consumer re-attempts delivery
  → repeat until success or max 5 retries → PermanentFailure
```

---

## 5. URL Rewriting

Email templates contain callback URLs (verification, password reset). Upstream services (e.g. IAM) may send URLs with internal addresses like `http://localhost:4001/verify?token=...`. When `FRONTEND_URL` is configured:

1. **Upstream provides full URL:** `rewrite_url()` replaces `scheme://host:port` with `FRONTEND_URL`, preserving `/path?query`.
2. **Upstream provides only token:** `build_callback_url()` constructs `{FRONTEND_URL}{path}?{query}` from scratch.
3. **No `FRONTEND_URL` configured:** Passes through upstream URL as-is.

---

## 6. SMTP HTML Auto-Detection

`SmtpProvider::send()` inspects the rendered content:

- **Content starts with `<!DOCTYPE` or `<html`:** Sends as `multipart/alternative` with:
  - `text/plain` part: stripped HTML via `html_to_text()` (removes scripts/styles/tags, decodes entities).
  - `text/html` part: original rendered content.
- **Otherwise:** Sends as `text/plain`.

TLS modes: `starttls` (default, port 587), `tls` (implicit, port 465), `none` (no TLS, for local Mailpit testing).

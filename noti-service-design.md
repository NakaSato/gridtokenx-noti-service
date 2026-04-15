# Design Spec: gridtokenx-noti-service
**Date:** 2026-04-15
**Status:** Draft
**Topic:** Notification Service (Email, SMS, Webhook, Push, WebSocket)

## 1. Executive Summary
The `gridtokenx-noti-service` is a centralized, stateful notification dispatcher designed to handle all outbound communication for the GridTokenX ecosystem. It provides a unified API and event-driven consumer for sending Email, SMS, Webhooks, Mobile/Web Push notifications, and real-time WebSocket alerts, maintaining a persistent audit trail and robust retry logic.

## 2. Goals & Success Criteria
- **Multi-channel support:** Support Email (SMTP/HTTP), SMS (Twilio), Webhooks (HTTP), Push (FCM), and WebSocket (Real-time).
- **Auditability:** Every notification request, attempt, and final status must be persisted in PostgreSQL.
- **Resilience:** Handle external provider failures with exponential backoff and retries.
- **Idempotency:** Prevent duplicate notifications for the same event via unique request/event IDs.
- **Observability:** Integrated with OpenTelemetry (tracing) and Prometheus (metrics).

## 3. Architecture
The service follows a "Stateful Unified Dispatcher" pattern, acting as a buffer between internal services and unreliable external providers.

### 3.1 Components
- **Inbound Adapters:**
    - `GrpcServer`: ConnectRPC implementation for synchronous requests (OTPs, urgent alerts).
    - `KafkaConsumer`: Subscribes to `user.events`, `trade.events`, and `settlement.events`.
    - `RabbitConsumer`: Processes `notif.queue` for batch/delayed tasks.
- **Core Orchestrator:**
    - `RequestProcessor`: Validates requests and persists initial `Pending` state.
    - `TemplateEngine`: Substitutes variables into localized templates (Handlebars/Tera).
    - `Dispatcher`: An async worker pool that executes delivery via Provider Adapters.
- **Provider Adapters:**
    - `EmailProvider`: Uses `lettre` for SMTP or provider-specific HTTP APIs.
    - `SmsProvider`: Integration with Twilio/Nexmo.
    - `PushProvider`: Integration with Firebase Cloud Messaging (FCM).
    - `WebSocketProvider`: Real-time browser/app alerts via direct connection management or Redis Pub/Sub.
    - `WebhookProvider`: Robust HTTP client with timeout/retry logic.
### 3.2 Caching (Redis)
The service utilizes Redis for high-performance transient data and coordination:
- **Template Cache:** Compiled versions of Handlebars/Tera templates to avoid repeated disk I/O and parsing overhead.
- **Idempotency Store:** Quick lookup of `idempotency_key` (with short TTL) before falling back to PostgreSQL.
- **Rate Limiting:** Tracking delivery frequency per recipient/channel to comply with provider (Twilio, SendGrid) limits.
- **WebSocket Registry:** Maps `user_id` to specific service instances for targeted message routing in a scaled environment.

### 3.3 Queuing Strategy
A hybrid approach is used to balance durability and flexibility:
- **Kafka (External Source):** Consumes high-volume business events (`user.events`, `trade.events`, `settlement.events`). Acts as the durable "log of record" for triggering notifications.
- **RabbitMQ (Internal Tasks):**
    - `notif.dispatch`: Primary queue for notifications ready for immediate delivery.
    - `notif.retry`: Delayed queue using `x-dead-letter-exchange` and message TTL to implement exponential backoff without blocking workers.
    - `notif.dead-letter`: Final resting place for notifications exceeding max retries for manual inspection.

### 3.4 Data Model (PostgreSQL)
```sql
CREATE TYPE notification_channel AS ENUM ('email', 'sms', 'push', 'webhook', 'websocket');
...
CREATE TYPE notification_status AS ENUM ('pending', 'processing', 'sent', 'delivered', 'failed', 'permanent_failure');

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID, -- Optional
    channel notification_channel NOT NULL,
    status notification_status NOT NULL DEFAULT 'pending',
    recipient TEXT NOT NULL, -- Email address, phone number, or webhook URL
    template_id TEXT NOT NULL,
    variables JSONB DEFAULT '{}',
    provider_id TEXT, -- e.g., 'twilio', 'sendgrid'
    provider_ref TEXT, -- ID from the external provider
    retry_count INT DEFAULT 0,
    next_retry_at TIMESTAMPTZ DEFAULT NOW(),
    error_message TEXT,
    idempotency_key TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX idx_notifications_status_retry ON notifications(status, next_retry_at) WHERE status = 'pending';
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
```

## 4. Workflows

### 4.1 Synchronous Request (gRPC)
1. Caller sends `SendNotificationRequest` via ConnectRPC.
2. `RequestProcessor` checks `idempotency_key`.
3. If new, persists to DB as `Pending`.
4. Returns `202 Accepted` with `notification_id`.
5. `Dispatcher` picks up and sends asynchronously.

### 4.2 Event-Driven Request (Kafka)
1. `KafkaConsumer` receives `TradeSettled` event.
2. Maps event to `trade_completion` template.
3. Persists to DB as `Pending`.
4. `Dispatcher` picks up and sends asynchronously.

### 4.3 Retry Logic
1. `Dispatcher` fails to send due to a 503 from Twilio.
2. `retry_count` incremented.
3. `next_retry_at` set to `now + (2 ^ retry_count)` minutes.
4. Status remains `Pending`.
5. Background job picks up record when `next_retry_at` <= `now`.

## 5. Security & Compliance
- **PII Protection:** Email addresses and phone numbers should be handled with care. Consider encryption at rest for the `recipient` field if required by PDPA.
- **Secrets:** API keys for Twilio, FCM, etc., stored in Vault and loaded via `config`.
- **mTLS:** All internal gRPC communication uses mTLS with SPIFFE identities.

## 6. Implementation Plan Highlights
- Scaffolding new Rust crate `gridtokenx-noti-service`.
- Defining Protobuf for notification service.
- Setting up SQLx migrations for PostgreSQL.
- Implementing the Dispatcher worker loop.
- Integrating with `tokio-tungstenite` or `axum` for WebSocket connectivity.
- Integrating with `rdkafka` and `lapin` (RabbitMQ).

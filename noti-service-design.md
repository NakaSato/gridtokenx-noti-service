# Design Spec: gridtokenx-noti-service
**Date:** 2026-05-24  
**Status:** Implemented  
**Topic:** Notification Service (Email, SMS, Webhook, Push, WebSocket)

---

## 1. Executive Summary
The `gridtokenx-noti-service` is a centralized, stateful notification dispatcher designed to handle all outbound communication for the GridTokenX ecosystem. It provides a unified REST/ConnectRPC API and event-driven consumer loops for sending Email, SMS, Webhooks, Mobile/Web Push notifications, and real-time WebSocket alerts, maintaining a persistent audit trail and robust retry logic.

---

## 2. Goals & Success Criteria
- **Multi-channel support:** Support Email (SMTP/lettre), SMS (Mock), Webhooks (Mock), Push (Mock), and WebSocket (Real-time).
- **Auditability:** Every notification request, attempt, and final status must be persisted in PostgreSQL.
- **Resilience:** Handle external provider failures with exponential backoff and retries via RabbitMQ.
- **Idempotency:** Prevent duplicate notifications for the same event via unique idempotency keys.
- **Observability:** Integrated with metrics (Prometheus) and logs (tracing).

---

## 3. Architecture
The service follows a "Stateful Unified Dispatcher" pattern, acting as a buffer between internal services and external providers.

### 3.1 Components
- **Inbound Adapters:**
    - `NotificationGrpcService`: ConnectRPC/gRPC implementation for synchronous requests.
    - `KafkaConsumer`: Subscribes to events on `iam.user.events` and `trading.trade.events`.
    - `RabbitMQConsumer`: Processes the `noti.dispatch` queue for sending tasks.
- **Core Orchestrator:**
    - `NotificationOrchestrator`: Validates requests, persists initial `Pending` state, resolved templates, and publishes delivery tasks.
    - `TemplateEngine`: Substitutes variables into Tera templates.
- **Provider Adapters:**
    - `SmtpProvider`: Uses `lettre` for SMTP delivery.
    - `WebSocketProvider`: Routes real-time alerts to connected browser/app client connections.
    - `MockProvider` (SMS, Push, Webhooks): Logs messages to console (acting as development mocks).

### 3.2 Caching (Redis)
Redis is utilized for high-performance transient state:
- **Caching Service:** Stores cached metadata, templates, and idempotency states.
- **WebSocket Registry:** Tracks active connection handles to route user-targeted push alerts directly.

### 3.3 Queuing Strategy
- **Kafka (External Event Source):** Consumes event logs (`iam.user.events` and `trading.trade.events`) to trigger user welcome messages, trade matches, and certificate issuances.
- **RabbitMQ (Internal Tasks):**
    - `noti.exchange`: Main direct exchange.
    - `noti.dispatch`: Primary queue where immediately runnable dispatch tasks are queued.
    - `noti.retry`: Delay queue utilizing RabbitMQ's message expiration (`expiration`/TTL) and dead-letter exchange configuration (`x-dead-letter-exchange = noti.exchange` & `x-dead-letter-routing-key = noti.dispatch`) to trigger asynchronous retries without blocking active threads.

### 3.4 Data Model (PostgreSQL)

```sql
CREATE TYPE notification_channel AS ENUM ('email', 'sms', 'push', 'webhook', 'websocket');

CREATE TYPE notification_status AS ENUM ('pending', 'processing', 'sent', 'delivered', 'failed', 'permanent_failure');

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID, -- Optional
    channel notification_channel NOT NULL,
    status notification_status NOT NULL DEFAULT 'pending',
    recipient TEXT NOT NULL, -- Email address, phone number, or webhook URL
    template_id TEXT NOT NULL,
    variables JSONB DEFAULT '{}',
    provider_id TEXT, -- e.g., 'smtp', 'mock-sms'
    provider_ref TEXT, -- ID from the external provider
    retry_count INT DEFAULT 0,
    next_retry_at TIMESTAMPTZ DEFAULT NOW(),
    error_message TEXT,
    idempotency_key TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    read_at TIMESTAMPTZ -- Added in migration 20260427000001
);

-- Indexes for performance
CREATE INDEX idx_notifications_status_retry ON notifications(status, next_retry_at) WHERE status = 'pending';
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
CREATE INDEX idx_notifications_user_unread ON notifications(user_id) WHERE read_at IS NULL; -- Added in migration 20260427000001
```

---

## 4. Workflows

### 4.1 Synchronous Request (gRPC/ConnectRPC)
1. Client sends `SendNotification` call.
2. Orchestrator checks `idempotency_key`.
3. If new, persists record in `notifications` table as `Pending`.
4. Publishes a `NotificationTask` to RabbitMQ's `noti.dispatch`.
5. Returns `Result` immediately to client.

### 4.2 Event-Driven Request (Kafka)
1. `KafkaConsumer` receives event payload:
   - **`UserRegistered`**: Triggers welcome email (`welcome.txt.tera`).
   - **`OrderMatched`**: Triggers WebSocket notification (`trade_matched.txt.tera`).
   - **`ErcIssued`**: Triggers email notification (`erc_issued.txt.tera`).
   - **`PasswordResetRequested`**: Triggers email (`password_reset.txt.tera`).
   - **`VerificationEmailRequested`**: Triggers HTML email (`verify_email.html.tera`).
2. Event data is mapped, notification saved as `Pending` with Kafka offset-based idempotency key.
3. Task published to RabbitMQ.

### 4.3 Retry Logic
1. If delivery fails (e.g. SMTP server transient error):
2. `retry_count` is incremented.
3. Task published to RabbitMQ `noti.retry` with TTL based on exponential backoff delay.
4. When TTL expires, RabbitMQ routes message back to `noti.dispatch` via DLX, and background consumer re-attempts delivery.
5. If max retries exceeded, status is updated to `PermanentFailure`.

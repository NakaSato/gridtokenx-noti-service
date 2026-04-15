# Notification Service

The **Notification Service** handles all outbound notifications for the GridTokenX platform — email delivery, templating, and delivery tracking.

---

## Architecture

The Notification Service uses a **layered DDD** architecture:

```
gridtokenx-noti-service/src/
├── main.rs                      # Entry point
├── lib.rs                       # Library root
├── config.rs                    # Service configuration
├── startup.rs                   # Server wiring, dependency injection
├── api/
│   ├── mod.rs                   # API module root
│   ├── server.rs                # Axum server, route definitions
│   └── handlers/                # Request handlers
├── domain/
│   ├── mod.rs                   # Domain module root
│   ├── entity.rs                # Notification entities (email, template)
│   ├── repository.rs            # Repository traits
│   └── service.rs               # Notification business logic
└── infrastructure/
    ├── mod.rs                    # Infrastructure module root
    ├── database.rs              # PostgreSQL repositories (SQLx)
    ├── cache.rs                 # Redis cache
    ├── templating.rs            # Email template engine
    ├── messaging/               # Kafka & RabbitMQ consumers/producers
    └── providers/               # External service providers
```

### Data Flow

```
API Request / Event → Domain Service → Template Engine → SMTP Delivery
                           ↓
                    Postgres (tracking)
```

---

## Features

| Feature | Description |
|:---|:---|
| **Email delivery** | Send transactional emails via SMTP (Mailpit in dev) |
| **Templating** | Dynamic email templates with variable substitution |
| **Delivery tracking** | Persist send/delivery/failure status in Postgres |
| **Caching** | Redis cache for templates and delivery status |

---

## Development

```bash
# Build
cd gridtokenx-noti-service && cargo build

# Test
cd gridtokenx-noti-service && cargo test

# Run migrations
just noti-migrate

# Create new migration
just noti-migrate-new name:add_templates
```

### Environment

| Variable | Default | Purpose |
|:---|:---|:---|
| `SMTP_HOST` | `mailpit` | SMTP server hostname |
| `SMTP_PORT` | `1025` | SMTP port |
| `SMTP_FROM` | `noreply@gridtokenx.com` | Sender email address |
| `MAILPIT_WEB_PORT` | `13060` | Mailpit web UI (dev email viewer) |

---

## Key Files

| What | Where |
|:---|:---|
| Entry point | [src/main.rs](src/main.rs) |
| Server wiring | [src/startup.rs](src/startup.rs) |
| API handlers | [src/api/server.rs](src/api/server.rs) |
| Business logic | [src/domain/service.rs](src/domain/service.rs) |
| Database layer | [src/infrastructure/database.rs](src/infrastructure/database.rs) |

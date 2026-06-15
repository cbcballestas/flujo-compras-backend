# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Infrastructure (must start first)
```bash
docker compose up -d
```

### Build all modules (run from repo root)
```bash
./mvnw clean install
```
> `order-service` must compile first — `inventory-service` and `notification-service` depend on its JAR for `OrderCreatedEvent`.

### Run individual services
```bash
cd order-service && ./mvnw spring-boot:run        # port 8080
cd inventory-service && ./mvnw spring-boot:run    # port 8081
cd notification-service && ./mvnw spring-boot:run # port 8082
```

### Run tests for a single module
```bash
./mvnw test -pl order-service
./mvnw test -pl inventory-service
./mvnw test -pl notification-service
```

## Architecture

**Choreographed Saga** over Apache Kafka. Three Spring Boot 3.5.14 / Java 21 services, each following **Hexagonal Architecture (Ports & Adapters)**:

```
domain/        → Pure models, enums, events, exceptions (no framework dependencies)
application/   → Port interfaces (port/in, port/out) + service implementations
infrastructure/→ Adapters: REST (in), Kafka/RabbitMQ consumers+producers (in/out), JPA/Mongo persistence (out)
```

### Services

| Service | Port | DB | Role |
|---|---|---|---|
| `order-service` | 8080 | MySQL 3306 | REST entry point; creates orders, publishes `OrderCreatedEvent` |
| `inventory-service` | 8081 | MySQL 3307 | Consumes orders; reserves stock; publishes success/failure events |
| `notification-service` | 8082 | MongoDB 27018 | Consumes inventory events; sends Gmail SMTP email |

### Intra-service communication pattern

Services do **not** call adapters directly. The application layer fires Spring `ApplicationEvent`s; infrastructure listeners translate those into Kafka/RabbitMQ sends:

```
UseCase → applicationEventPublisher.publishEvent(event)
                ↓
     @EventListener in infrastructure Listener
                ↓
     KafkaTemplate / RabbitTemplate → broker
```

### Cross-service dependency

`inventory-service` imports `OrderCreatedEvent` directly from the `order-service` JAR (Maven dependency). Both services must be compiled for `inventory-service` to build.

### Retry & DLT strategy (inventory-service)

`InventoryKafkaAdapter` uses `@RetryableTopic` with 2 retries (3 s / 6 s backoff). After exhausting retries, `@DltHandler` re-sends the message to the original topic up to 3 more times via a custom `nroRetry` header. On the 4th DLT arrival (nroRetry ≥ 3), the event is persisted to the outbox table and a `FailedInventoryEvent` is published.

Retryable exceptions: `InsufficientStockException`, `ProductNotFoundException`, `NullPointerException`.

### Outbox pattern

`inventory-service` persists failed events in an `outbox_event` table (JPA/MySQL) and also publishes a backup via RabbitMQ. `notification-service` has a parallel `OutBoxEvent` in MongoDB.

### Database migrations

Only `inventory-service` uses **Flyway** (`src/main/resources/db/migration/`):
- `V1__create_inventory_table.sql` — schema
- `V2__add_initial_inventory.sql` — seed data (PROD-001, PROD-002, PROD-003)

`order-service` uses `ddl-auto: update` (no Flyway).

## Key Kafka topics

| Topic | Producer | Consumer |
|---|---|---|
| `orders-created-topic` | order-service | inventory-service |
| `inventory-reserved-topic` | inventory-service | notification-service |
| `inventory-reservation-failed-topic` | inventory-service | order-service |
| `orders-created-topic-dlt` | Spring Kafka | inventory-service `@DltHandler` |

All producers are configured with `enable.idempotence=true`, `acks=all`, `retries=3`, `max.in.flight.requests.per.connection=1`.

## Environment variables

All services support overriding via env vars (defaults shown in `application.yaml`):

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVER` | `localhost:29092` | all |
| `DB_URL` | `jdbc:mysql://localhost:3306/orders_db` | order-service |
| `DB_URL` | `jdbc:mysql://localhost:3307/inventory_db` | inventory-service |
| `DB_USER` / `DB_PASSWORD` | `root` / `123456` | order/inventory |
| `MONGO_HOST` / `MONGO_PORT` / `MONGO_DB` | `localhost` / `27018` / `notification_db` | notification-service |
| `MAIL_USERNAME` / `MAIL_PASSWORD` | placeholder | notification-service |

Email credentials must be set before running `notification-service`. Use a Gmail App Password (not the account password).

## MapStruct + Lombok

Both are wired as annotation processor paths in the root `pom.xml`. Lombok must be declared **before** MapStruct in the processor path (enforced via `lombok-mapstruct-binding`). Do not reorder them.

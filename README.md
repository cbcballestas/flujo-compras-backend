# Microservicios con Kafka — Patrón Saga

Sistema de gestión de órdenes basado en microservicios que implementa el **Patrón Saga Coreografiado** usando Apache Kafka como bus de eventos.

## Tecnologías

- **Java 17** + **Spring Boot 3.x**
- **Apache Kafka** (Confluent Platform 7.0.1)
- **Maven** (multi-módulo)
- **MySQL 8** (order-service e inventory-service)
- **PostgreSQL 15** (payment-service)
- **MongoDB 7** (notification-service)
- **Lombok**, **MapStruct**, **JPA**

---

## Arquitectura

El proyecto sigue la **Arquitectura Hexagonal (Puertos y Adaptadores)** en cada servicio:

```
domain/          → Modelos de dominio, enums, eventos, excepciones (sin dependencias de framework)
application/     → Interfaces de casos de uso (port/in), puertos de salida (port/out), servicios
infrastructure/  → Adaptadores: REST (in), Kafka consumers/producers (in/out), persistencia JPA/Mongo (out)
```

### Servicios

| Servicio             | Puerto | Base de Datos       | Rol                                                                  |
|----------------------|--------|---------------------|----------------------------------------------------------------------|
| `order-service`      | 8080   | MySQL (3306)        | Punto de entrada REST; crea órdenes y publica eventos en Kafka       |
| `inventory-service`  | 8081   | MySQL (3307)        | Consume eventos de órdenes; reserva stock; publica éxito o fallo     |
| `notification-service` | 8082   | MongoDB (27017)     | Consume eventos de inventario; envía notificaciones por email        |

### Diagrama de flujo

```
┌─────────┐        REST POST /orders
│ Cliente │ ─────────────────────────────────────────────────────────────────┐
└─────────┘                                                                   │
                                                                              ▼
                                                              ┌───────────────────────────┐
                                                              │       order-service        │
                                                              │  1. Persiste orden (CREATED)│
                                                              │  2. ApplicationEvent       │
                                                              │  3. OrderListener →        │
                                                              │     publica a Kafka        │
                                                              └───────────┬───────────────┘
                                                                          │
                                                                          │ orders-created
                                                                          ▼
                                                              ┌───────────────────────────┐
                                                              │     inventory-service      │
                                                              │  1. Consume orders-created │
                                                              │  2. Reserva stock          │
                                                              │  3. ApplicationEvent       │
                                                              │     ├─ ReservedInventory → │
                                                              │     │  inventory-reserved  │
                                                              │     └─ FailedInventory →   │
                                                              │        inventory-failed    │
                                                              └──────┬────────────────┬───┘
                                                                     │                │
                                                      inventory-reserved        inventory-failed
                                                                     │                │
                                                                     ▼                ▼
                                                       ┌───────────────────┐  ┌──────────────────┐
                                                       │notification-service│  │  order-service   │
                                                       │ Envía email al     │  │ Actualiza orden  │
                                                       │ cliente           │  │ a CANCELLED/FAILED│
                                                       └───────────────────┘  └──────────────────┘
```

### Comunicación interna dentro de cada servicio

La comunicación entre capas **no usa llamadas directas** sino `ApplicationEventPublisher` de Spring. El adaptador de salida escucha esos eventos y los traduce en mensajes Kafka:

```
UseCase → applicationEventPublisher.publishEvent(event)
                    ↓
         @EventListener en Listener
                    ↓
         Adaptador Kafka → producer.send(...)
```

---

## Tópicos de Kafka

| Tópico                  | Particiones | Productor            | Consumidor(es)                      | Descripción                                        |
|-------------------------|-------------|----------------------|-------------------------------------|----------------------------------------------------|
| `orders-created-topic`        | 3           | order-service        | inventory-service                   | Orden recién creada; dispara la reserva de stock   |
| `orders-created-topic-try`    | —           | inventory-service    | inventory-service                   | Reintentos automáticos (`@RetryableTopic`)         |
| `orders-created-topic-dlt`    | —           | inventory-service    | inventory-service (`@DltHandler`)   | Dead Letter Topic tras agotar reintentos           |
| `orders-confirmed`      | 3           | order-service        | —                                   | Orden confirmada tras pago exitoso (futuro)        |
| `orders-cancelled`      | 2           | order-service        | —                                   | Orden cancelada                                    |
| `orders-completed`      | 3           | order-service        | —                                   | Orden completada                                   |
| `inventory-reserved`    | 3           | inventory-service    | notification-service                | Stock reservado correctamente                      |
| `inventory-failed`      | 3           | inventory-service    | order-service                       | Reserva de stock fallida                           |
| `log-error`             | 3           | order-service, otros | —                                   | Registro de errores de publicación Kafka           |

### Estrategia de reintentos en `inventory-service`

```
orders-created  →  [intento 1]
                →  orders-created-try-0  (backoff 3s)
                →  orders-created-try-1  (backoff 6s)
                →  orders-created-dlt    (@DltHandler)
                       ├─ si nroRetry < 3 → reenvía a orders-created con header nroRetry++
                       └─ si nroRetry >= 3 → publica FailedInventoryEvent
```

Excepciones reintentables: `NullPointerException`, `InsufficientStockException`, `ProductWithoutInventoryException`.

### Configuración de productores

Todos los productores comparten la misma configuración para garantizar exactamente una entrega:

| Propiedad                              | Valor   |
|----------------------------------------|---------|
| `enable.idempotence`                   | `true`  |
| `acks`                                 | `all`   |
| `retries`                              | `3`     |
| `max.in.flight.requests.per.connection`| `1`     |

---

## Estructura de eventos

### `OrderCreatedEvent` (order-service → Kafka)
```json
{
  "orderId": "uuid",
  "customerId": "cust-001",
  "items": [
    { "productId": "prod-001", "productName": "Laptop", "quantity": 1, "price": 1200.00 }
  ],
  "totalAmount": 1200.00,
  "status": "CREATED",
  "createdAt": "2026-04-18T10:30:00",
  "retryCount": 0,
  "errorMessage": null
}
```

### `ReservedInventoryEvent` (inventory-service → Kafka)
```json
{
  "orderId": "uuid",
  "reservationId": "res-uuid",
  "customerId": "cust-001",
  "items": [
    { "productId": "prod-001", "quantity": 1 }
  ],
  "totalAmount": 1200.00,
  "status": "RESERVED"
}
```

### `FailedInventoryEvent` (inventory-service → Kafka)
```json
{
  "orderId": "uuid",
  "status": "FAILED",
  "errorMessage": "Insufficient stock for productId=prod-001"
}
```

### Estados de una orden (`OrderStatus`)

```
CREATED → PENDING_INVENTORY → INVENTORY_RESERVED → PENDING_PAYMENT
                           → PAYMENT_PROCESSED → CONFIRMED → COMPLETED
                           → CANCELLED / FAILED
```

---

## API REST — order-service

Base URL: `http://localhost:8080`

### Crear orden

```http
POST /orders
Content-Type: application/json

{
  "customerId": "cust-001",
  "items": [
    {
      "productId": "prod-001",
      "productName": "Laptop Dell XPS",
      "quantity": 1,
      "price": 1200.00
    },
    {
      "productId": "prod-002",
      "productName": "Mouse Logitech",
      "quantity": 2,
      "price": 25.00
    }
  ]
}
```

**Respuesta 200 OK:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "customerId": "cust-001",
  "items": [...],
  "totalAmount": 1250.00,
  "status": "CREATED",
  "createdAt": "2026-04-18T10:30:00"
}
```

### Obtener todas las órdenes

```http
GET /orders
```

### Obtener orden por ID

```http
GET /orders/{id}
```

---

## Casos de ejemplo

### Caso 1: Orden exitosa (stock disponible)

```
1. Cliente → POST /orders  (customerId=cust-001, 1x Laptop $1200)
2. order-service persiste orden con status=CREATED
3. order-service publica OrderCreatedEvent → orders-created
4. inventory-service consume el evento → reserva 1x Laptop
5. inventory-service publica ReservedInventoryEvent → inventory-reserved
6. notification-service consume inventory-reserved → envía email de confirmación al cliente
```

### Caso 2: Orden fallida (sin stock)

```
1. Cliente → POST /orders  (customerId=cust-002, 10x Laptop $1200)
2. order-service persiste orden con status=CREATED
3. order-service publica OrderCreatedEvent → orders-created
4. inventory-service consume el evento → lanza InsufficientStockException
5. @RetryableTopic reintenta 2 veces (3s y 6s de backoff)
6. Tras agotar reintentos → orders-created-dlt
7. @DltHandler reenvía hasta 3 veces con header nroRetry
8. Tras 3 intentos en DLT → publica FailedInventoryEvent → inventory-failed
9. order-service consume inventory-failed → actualiza orden a status=FAILED/CANCELLED
```

### Caso 3: Error transitorio (NullPointerException)

```
1. inventory-service recibe evento con datos incompletos
2. Lanza NullPointerException (reintentable)
3. Primer reintento → orders-created-try-0 (espera 3s)
4. Segundo reintento → orders-created-try-1 (espera 6s)
5. Si el error persiste → orders-created-dlt → ciclo DLT
6. Si se resuelve en algún reintento → flujo exitoso continúa normalmente
```

---

## Ejecución local

### 1. Levantar infraestructura

```bash
docker compose up -d
```

Servicios disponibles:
- Kafka broker: `localhost:29092`
- Kafka UI: `http://localhost:8088`
- MySQL (orders): `localhost:3306`
- MySQL (inventory): `localhost:3307`
- PostgreSQL (payment): `localhost:5432`
- MongoDB (notification): `localhost:27017`

### 2. Compilar todos los módulos

```bash
./mvnw clean install
```

> `order-service` debe compilarse primero ya que `inventory-service` depende de su JAR para el DTO `OrderCreatedEvent`.

### 3. Iniciar cada servicio

```bash
# Terminal 1
cd order-service && ./mvnw spring-boot:run

# Terminal 2
cd inventory-service && ./mvnw spring-boot:run

# Terminal 3
cd notification-service && ./mvnw spring-boot:run
```

### 4. Probar el flujo completo

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-001",
    "items": [
      {
        "productId": "prod-001",
        "productName": "Laptop Dell XPS",
        "quantity": 1,
        "price": 1200.00
      }
    ]
  }'
```

Monitorear los eventos en **Kafka UI** → `http://localhost:8088`.

---

## Tests

```bash
# Todos los tests
./mvnw test

# Tests de un servicio específico
cd order-service && ./mvnw test

# Clase de test específica
cd order-service && ./mvnw test -Dtest=CreateOrderUseCaseImplTest
```

Los tests unitarios usan **JUnit 5 + Mockito + AssertJ**. Se usa `@EmbeddedKafka` solo cuando la integración con Kafka es estrictamente necesaria.

---

## Estructura del proyecto

```
microservices/
├── docker-compose.yml
├── pom.xml                          # POM raíz (multi-módulo)
├── lombok.config
├── order-service/
│   └── src/main/java/.../
│       ├── domain/                  # Order, OrderItem, OrderStatus, OrderCreatedEvent
│       ├── application/             # CreateOrderUseCase, GetOrderUseCase, servicios
│       └── infrastructure/
│           ├── adapter/in/rest/     # OrderController
│           └── adapter/out/
│               ├── messaging/       # InventoryServiceMessagingAdapter, OrderListener
│               └── persistence/     # OrderPersistenceAdapter (JPA/MySQL)
├── inventory-service/
│   └── src/main/java/.../
│       ├── domain/                  # ReservedInventoryEvent, FailedInventoryEvent
│       ├── application/             # InventoryMessagingPort y servicios
│       └── infrastructure/
│           ├── adapter/in/messaging/ # InventoryKafkaAdapter (@RetryableTopic, @DltHandler)
│           └── adapter/out/
│               ├── messaging/        # NotificationServiceMessagingAdapter, OrderServiceMessagingAdapter
│               └── persistence/      # (JPA/MySQL)
├── notification-service/
│   └── src/main/java/.../
│       └── infrastructure/          # KafkaConfig, consumer de inventory-reserved
```

# CLAUDE.md

Este archivo proporciona orientación a Claude Code (claude.ai/code) cuando trabaja con el código de este repositorio.

## Comandos de Compilación y Ejecución

```bash
# Compilar todos los módulos desde la raíz
./mvnw clean install

# Compilar un módulo específico
cd order-service && ./mvnw clean install

# Ejecutar un servicio específico
cd order-service && ./mvnw spring-boot:run

# Ejecutar todos los tests
./mvnw test

# Ejecutar tests de un módulo específico
cd order-service && ./mvnw test

# Ejecutar una clase de test específica
cd order-service && ./mvnw test -Dtest=CreateOrderUseCaseImplTest

# Iniciar la infraestructura (Kafka + bases de datos)
docker compose up -d

# Detener la infraestructura
docker compose down
```

## Arquitectura

Es un proyecto Maven multi-módulo (`groupId: com.cballestas`) con cuatro microservicios Spring Boot 3.x, cada uno siguiendo la **Arquitectura Hexagonal (Puertos y Adaptadores)**:

```
domain/          → Modelos de dominio puros, enums, eventos, excepciones (sin dependencias de framework)
application/     → Interfaces de casos de uso (port/in) + interfaces de puertos de salida (port/out) + implementaciones de servicios
infrastructure/  → Adaptadores: controladores REST (in), consumidores/productores Kafka (in/out), persistencia JPA/Mongo (out)
```

### Servicios y sus Roles

| Servicio | Puerto | BD | Rol |
|---|---|---|---|
| `order-service` | 8080 | MySQL (3306) | Punto de entrada REST; crea órdenes y publica en Kafka |
| `inventory-service` | — | MySQL (3307) | Consume eventos de órdenes; reserva stock; publica éxito/fallo |
| `payment-service` | — | PostgreSQL (5432) | Stub; destinado a procesar pagos (aún no implementado) |
| `notification-service` | — | MongoDB (27017) | Consume eventos; envía notificaciones por email vía Spring Mail |

### Flujo de Eventos Kafka (Patrón Saga)

```
[Cliente]
  → POST /orders
    → order-service guarda la orden (CREATED)
    → Spring ApplicationEvent → OrderListener
      → publica OrderCreatedEvent al topic: orders-created

[inventory-service]
  ← consume orders-created (con @RetryableTopic: 2 intentos, backoff 3s/6s, sufijo DLT -dlt)
  → reserva inventario
  → Spring ApplicationEvent → NotificationServiceListener / OrderServiceListener
    → publica ReservedInventoryEvent al topic: inventory-reserved
    → publica FailedInventoryEvent al topic: inventory-failed
```

**Importante**: Dentro de cada servicio, la comunicación entre componentes usa `ApplicationEventPublisher` de Spring (no llamadas directas a métodos); luego el adaptador de salida traduce esos eventos en mensajes Kafka.

### Dependencia entre Módulos

`inventory-service` depende directamente del JAR compilado de `order-service` para el DTO `OrderCreatedEvent`. El build de `order-service` usa `classifier=exec` para el fat JAR y también produce un `test-jar` para compartir fixtures de prueba.

### Configuración de Topics Kafka

Los topics se definen como beans en `KafkaConfig`. Topics principales (configurados vía propiedades `app.kafka.topics.*`):
- `orders-created` — 3 particiones
- `orders-confirmed`, `orders-completed` — 3 particiones
- `orders-cancelled` — 2 particiones
- `inventory-reserved`, `inventory-failed` — 3 particiones
- `log-error` — 3 particiones

Broker Kafka: `localhost:29092` (externo), `kafka:9092` (red Docker interna). Kafka UI disponible en `http://localhost:8088`.

### Configuración de Productores Kafka

Los productores usan idempotencia (`enable.idempotence=true`), `acks=all`, `retries=3`, `max.in.flight.requests.per.connection=1`. Dos tipos de productor por servicio: `KafkaTemplate<String, String>` (serializado a JSON vía ObjectMapper) y `KafkaProducer<String, EventType>` tipado (Jackson JsonSerializer).

## Convenciones Clave

- **Lombok** se usa extensivamente: `@RequiredArgsConstructor`, `@Slf4j`, `@Builder`, `@Data`.
- Los mappers de **MapStruct** gestionan las conversiones entre modelos de dominio y DTOs/entidades. El orden del procesador de anotaciones importa: mapstruct-processor → lombok → lombok-mapstruct-binding.
- Los modelos de dominio son clases Java simples (sin anotaciones JPA); las entidades de persistencia son clases separadas en `infrastructure/adapter/out/persistence/entity/`.
- `lombok.config` en la raíz configura el comportamiento de Lombok para todo el proyecto.

## Guía de Testing

Los tests usan JUnit 5 + Mockito + AssertJ. Se prefieren tests unitarios puros con mocks sobre `@SpringBootTest`. Usar `@EmbeddedKafka` solo cuando la integración con Kafka sea estrictamente necesaria.

Convención de nombres: `debe[AccionEsperada]Cuando[Condicion]` con `@DisplayName` en español. Agrupar escenarios relacionados con `@Nested`. Cobertura objetivo: ≥90% por clase.

El prompt `.github/prompts/add-unit-tests.prompt.md` puede usarse para generar tests automáticamente para un archivo.

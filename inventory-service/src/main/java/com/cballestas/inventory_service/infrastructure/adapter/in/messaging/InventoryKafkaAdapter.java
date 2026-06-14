package com.cballestas.inventory_service.infrastructure.adapter.in.messaging;

import com.cballestas.inventory_service.application.port.in.InventoryMessagingPort;
import com.cballestas.inventory_service.domain.exception.InsufficientStockException;
import com.cballestas.inventory_service.domain.exception.ProductNotFoundException;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryKafkaAdapter {

    public static final String RETRY_HEADER = "nroRetry";
    public static final int MAX_RETRY_ATTEMPTS = 3;

    private final InventoryMessagingPort inventoryMessagingPort;

    @Qualifier("orderCreatedEventTemplate")
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Value("${app.kafka.topics.orders-created}")
    private String orderCreatedTopic;

    /**
     * Consume eventos de órdenes creadas desde el tópico Kafka configurado.
     * Al recibir un evento, lo procesa delegando la lógica al caso de uso de inventario.
     *
     * @param orderCreatedEvent evento recibido desde Kafka que contiene los datos de la orden creada
     */
    @RetryableTopic(
            kafkaTemplate = "orderCreatedEventTemplate",
            listenerContainerFactory = "orderCreatedListenerContainerFactory",
            include = {
                    ProductNotFoundException.class,
                    InsufficientStockException.class
            },
            backoff = @Backoff(delay = 3000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = "-try", dltTopicSuffix = "-dlt")
    @KafkaListener(topics = {"${app.kafka.topics.orders-created}"},
            groupId = "${app.kafka.group-id.inventory-group-id}",
            containerFactory = "orderCreatedListenerContainerFactory")
    void consumeOrderCreatedEvent(OrderCreatedEvent orderCreatedEvent) {
        log.info("Received Order Created Event: {}", orderCreatedEvent);
        inventoryMessagingPort.handleOrderCreated(orderCreatedEvent);
    }

    /**
     * Maneja los mensajes que han fallado tras los reintentos y han sido enviados al tópico DLT (Dead Letter Topic).
     * Si el número de reintentos es menor a 3, reenvía el mensaje al tópico original incrementando el contador de reintentos.
     * Si se supera el límite, persiste el evento en la base de datos mediante el outbox.
     *
     * @param orderCreatedEvent evento recibido que no pudo ser procesado exitosamente
     * @param topic nombre del tópico donde se recibió el mensaje DLT
     * @param nroRetry número de reintentos realizados (puede ser nulo)
     */
    @DltHandler
    public void handleDltOrderCreatedEvent(
            OrderCreatedEvent orderCreatedEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = RETRY_HEADER, required = false) String nroRetry) {
        log.info("Retries attempts: {}", nroRetry);
        int iNroRetry = ((nroRetry == null) ? 0 : Integer.parseInt(nroRetry));
        log.info("Message dlt topic={}, payload={}", topic, orderCreatedEvent);

        if (iNroRetry < MAX_RETRY_ATTEMPTS) {

            int nextAttempt = iNroRetry + 1;

            Message<OrderCreatedEvent> message = MessageBuilder
                    .withPayload(orderCreatedEvent)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader(KafkaHeaders.KEY, StringUtils.hasText(orderCreatedEvent.orderId()) ? orderCreatedEvent.orderId() : "unknown-key")
                    .setHeader(RETRY_HEADER, String.valueOf(nextAttempt))
                    .setHeader(KafkaHeaders.EXCEPTION_MESSAGE, exceptionMessage)
                    .setHeader("contentType", "application/json") // Útil para tu configuración de DLT
                    .build();
            kafkaTemplate.send(message)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("❌ Error enviando mensaje a Kafka en reintento #{}: {}",
                                    nextAttempt, ex.getMessage(), ex);
                        } else {
                            log.info("✅ Mensaje reenviado exitosamente. Intento #{} de {}",
                                    nextAttempt, MAX_RETRY_ATTEMPTS);
                        }
                    });
        } else {
            // Database Persiste(NO_SQL - MongoDb/ Casandra) or File Serve
            log.error("❌ Max retry attempts reached for order: {}. Saving to outbox with error message: {}",
                    orderCreatedEvent.orderId(), exceptionMessage);

            OrderCreatedEvent outboxEvent = OrderCreatedEvent.builder()
                    .orderId(orderCreatedEvent.orderId())
                    .customerId(orderCreatedEvent.customerId())
                    .items(orderCreatedEvent.items())
                    .totalAmount(orderCreatedEvent.totalAmount())
                    .status(orderCreatedEvent.status())
                    .createdAt(orderCreatedEvent.createdAt())
                    .retryCount(iNroRetry)
                    .errorMessage(exceptionMessage)
                    .build();

            inventoryMessagingPort.saveOutboxEvent(orderCreatedTopic, outboxEvent, exceptionMessage);
        }
    }
}

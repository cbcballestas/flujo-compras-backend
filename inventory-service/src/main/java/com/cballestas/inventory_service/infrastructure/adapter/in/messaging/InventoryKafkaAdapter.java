package com.cballestas.inventory_service.infrastructure.adapter.in.messaging;

import com.cballestas.inventory_service.application.port.in.InventoryMessagingPort;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryKafkaAdapter {

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
                    KafkaException.class,
                    KafkaProducerException.class
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
            @Header(value = "nroRetry", required = false) String nroRetry) {
        log.info("Retries attempts: {}", nroRetry);
        int iNroRetry = ((nroRetry == null) ? 0 : Integer.parseInt(nroRetry));
        log.info("Message dlt topic={}, payload={}", topic, orderCreatedEvent);

        if (iNroRetry < 3) {
            Map<String, Object> headers = Map.of(
                    KafkaHeaders.TOPIC, orderCreatedTopic,
                    KafkaHeaders.KEY, orderCreatedEvent.orderId(),
                    "nroRetry", (iNroRetry + 1),
                    "contentType", "application/json");
            kafkaTemplate.send(new GenericMessage<>(orderCreatedEvent, new MessageHeaders(headers)));
        } else {
            // Database Persiste(NO_SQL - MongoDb/ Casandra) or File Serve
            log.info("Persist event to database: {}", orderCreatedEvent);
            inventoryMessagingPort.saveOutboxEvent(orderCreatedTopic, orderCreatedEvent);
        }
    }
}

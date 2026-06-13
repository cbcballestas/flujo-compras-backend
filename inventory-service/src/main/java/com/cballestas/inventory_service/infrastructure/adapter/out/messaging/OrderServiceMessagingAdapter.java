package com.cballestas.inventory_service.infrastructure.adapter.out.messaging;

import com.cballestas.inventory_service.application.port.out.OrderServiceMessagingPort;
import com.cballestas.inventory_service.domain.model.event.FailedInventoryEvent;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Adaptador de mensajería para la comunicación con el servicio de órdenes.
 * Publica eventos relacionados con el inventario fallido en el tópico Kafka correspondiente.
 * Implementa el puerto de salida {@link OrderServiceMessagingPort}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderServiceMessagingAdapter implements OrderServiceMessagingPort {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Topicos
    @Value("${app.kafka.topics.inventory-failed}")
    private String inventoryFailedTopic;

    @Value("${app.kafka.topics.orders-backup}")
    private String ordersBackupTopic;

    /**
     * Publica un evento de inventario fallido en el tópico Kafka configurado.
     * Serializa el evento a JSON y lo envía usando el orderId como clave.
     *
     * @param event evento {@link FailedInventoryEvent} que representa el fallo en la reserva de inventario
     * @throws IllegalArgumentException si el evento es nulo
     * @throws RuntimeException si ocurre un error de serialización o envío a Kafka
     */
    @Override
    public void publishFailedReservedInventoryEvent(FailedInventoryEvent event) {
        try {
            var failedInventoryEvent = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(inventoryFailedTopic, event.orderId(), failedInventoryEvent)
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            log.error("Error publishing Failed Reserved Inventory Event", ex);
                        }
                        log.info("Publishing Failed Reserved Inventory Event: {}", failedInventoryEvent);
                    });
        } catch (JsonProcessingException e) {
            log.error("Error serializing Failed Reserved Inventory Event", e);
        }
    }

    /**
     * Publica un evento de tipo parking lot (respaldo de orden) en el tópico Kafka configurado.
     * Serializa el evento {@link OrderCreatedEvent} a JSON y lo envía usando el orderId como clave.
     *
     * @param event evento {@link OrderCreatedEvent} que representa la orden a respaldar en el parking lot
     * @throws IllegalArgumentException si el evento es nulo
     * @throws RuntimeException si ocurre un error de serialización o envío a Kafka
     */
    @Override
    public void publishParkingLotEvent(OrderCreatedEvent event) {
        try {
            var orderParkingLotEvent = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(ordersBackupTopic, event.orderId(), orderParkingLotEvent)
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            log.error("Error publishing Parking Lot Event", ex);
                        }
                        log.info("Publishing Parking Lot Event: {}", event);
                    });
        } catch (JsonProcessingException e) {
            log.error("Error serializing Parking Lot Event", e);
        }
    }
}

package com.cballestas.inventory_service.application.service;

import com.cballestas.inventory_service.application.port.in.InventoryMessagingPort;
import com.cballestas.inventory_service.application.port.out.InventoryPersistencePort;
import com.cballestas.inventory_service.application.port.out.InventoryReservationPersistencePort;
import com.cballestas.inventory_service.application.port.out.OutboxEventPersistencePort;
import com.cballestas.inventory_service.domain.enums.ReservationStatus;
import com.cballestas.inventory_service.domain.exception.InsufficientStockException;
import com.cballestas.inventory_service.domain.exception.ProductNotFoundException;
import com.cballestas.inventory_service.domain.model.Inventory;
import com.cballestas.inventory_service.domain.model.InventoryReservation;
import com.cballestas.inventory_service.domain.model.event.FailedInventoryEvent;
import com.cballestas.inventory_service.domain.model.event.OutBoxEvent;
import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.inventory_service.domain.model.event.ReservedItemEvent;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService implements InventoryMessagingPort {

    private final InventoryPersistencePort inventoryPersistencePort;
    private final InventoryReservationPersistencePort inventoryReservationPersistencePort;
    private final OutboxEventPersistencePort outboxEventPersistencePort;

    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher applicationEventPublisher;

    /**
     * Procesa la creación de una orden, verificando la disponibilidad de inventario para todos los productos solicitados.
     * Si hay suficiente inventario, realiza la reserva, persiste los cambios y publica un evento de éxito.
     * Si no hay suficiente inventario, publica un evento de fallo.
     *
     * @param orderCreatedEvent evento que contiene los datos de la orden creada, incluyendo los productos y cantidades solicitadas
     */
    @Transactional
    @Override
    public void handleOrderCreated(OrderCreatedEvent orderCreatedEvent) {
        log.info("Processing inventory reservation for order: {}", orderCreatedEvent.orderId());

        if (!canReserveAllItems(orderCreatedEvent)) {
            log.warn("Inventory reservation failed for order: {}", orderCreatedEvent.orderId());
            publishFailedInventoryEvent(orderCreatedEvent, "Insufficient inventory for one or more products");
            return;
        }

        List<ReservedItemEvent> reservedItemEvents = reserveAllItems(orderCreatedEvent);
        publishReservedInventoryEvent(orderCreatedEvent, reservedItemEvents);
        log.info("Inventory reserved successfully for order: {}", orderCreatedEvent.orderId());

        log.info("Saving inventory processed order: {}", orderCreatedEvent.orderId());
        applicationEventPublisher.publishEvent(orderCreatedEvent.orderId());
    }

    /**
     * Persiste un evento en la tabla outbox para garantizar la consistencia eventual entre servicios.
     * Serializa el evento de orden creada y lo almacena junto con el nombre del tópico.
     *
     * @param topic             nombre del tópico Kafka al que se enviará el evento
     * @param orderCreatedEvent evento que contiene los datos de la orden creada
     * @param errorMessage      mensaje descriptivo del motivo del fallo, si aplica (puede ser nulo o vacío si no hay error)
     */
    @Transactional
    @Override
    public void saveOutboxEvent(String topic, OrderCreatedEvent orderCreatedEvent, String errorMessage) {
        try {
            var outboxEvent = OutBoxEvent.builder()
                    .key(orderCreatedEvent.orderId())
                    .topic(topic)
                    .payload(objectMapper.writeValueAsString(orderCreatedEvent))
                    .errorMessage(errorMessage)
                    .published(Boolean.FALSE)
                    .build();
            log.info("Saving outbox event to database, for order: {}", orderCreatedEvent.orderId());
            outboxEventPersistencePort.save(outboxEvent);

            log.info("Saving backup outbox event for order: {}", orderCreatedEvent.orderId());
            applicationEventPublisher.publishEvent(outboxEvent);
        } catch (JsonProcessingException e) {
            log.error("Error while saving outbox event: {}", e.getMessage());
        }
    }

    /**
     * Verifica si todos los productos del pedido pueden ser reservados en el inventario.
     *
     * @param orderCreatedEvent evento con los productos y cantidades a reservar
     * @return {@code true} si todos los productos pueden ser reservados, {@code false} en caso contrario
     */
    private boolean canReserveAllItems(OrderCreatedEvent orderCreatedEvent) {
        for (var item : orderCreatedEvent.items()) {
            Optional<Inventory> byProductIdOpt = inventoryPersistencePort.findByProductId(item.productId());
            if (byProductIdOpt.isEmpty()) {
                log.warn("Product {} does not exist in inventory", item.productId());
                throw new ProductNotFoundException("Product " + item.productId() + " does not exist in inventory");
            }
            Inventory inventory = byProductIdOpt.get();
            if (!inventory.canReserve(item.quantity())) {
                log.warn("Insufficient stock for product {}: available={}, requested={}",
                        item.productId(), inventory.getAvailable(), item.quantity());
                throw new InsufficientStockException("Insufficient stock for product " + item.productId());
            }
        }
        return true;
    }

    /**
     * Reserva todos los productos del pedido, persiste los cambios y construye la lista de eventos reservados.
     *
     * @param orderCreatedEvent evento con los productos y cantidades a reservar
     * @return lista de eventos {@link ReservedItemEvent} que representan los productos reservados
     */
    private List<ReservedItemEvent> reserveAllItems(OrderCreatedEvent orderCreatedEvent) {
        List<ReservedItemEvent> reservedItemEvents = new ArrayList<>();
        for (var orderItem : orderCreatedEvent.items()) {

            Optional<Inventory> byProductOpt = inventoryPersistencePort.findByProductId(orderItem.productId());

            if (byProductOpt.isPresent()) {
                Inventory inventory = byProductOpt.get();
                inventory.reserve(orderItem.quantity());
                inventoryPersistencePort.save(inventory);

                InventoryReservation reservation = InventoryReservation.builder()
                        .orderId(orderCreatedEvent.orderId())
                        .productId(orderItem.productId())
                        .quantity(orderItem.quantity())
                        .status(ReservationStatus.RESERVED)
                        .build();
                inventoryReservationPersistencePort.save(reservation);

                reservedItemEvents.add(ReservedItemEvent.builder()
                        .productId(orderItem.productId())
                        .quantity(orderItem.quantity())
                        .productName(inventory.getProductName())
                        .price(orderItem.price())
                        .build());
            }

        }
        return reservedItemEvents;
    }

    /**
     * Publica un evento de inventario reservado exitosamente.
     *
     * @param orderCreatedEvent  evento original de la orden creada
     * @param reservedItemEvents lista de productos reservados exitosamente
     */
    private void publishReservedInventoryEvent(OrderCreatedEvent orderCreatedEvent, List<ReservedItemEvent> reservedItemEvents) {
        ReservedInventoryEvent event = ReservedInventoryEvent.builder()
                .orderId(orderCreatedEvent.orderId())
                .reservationId(UUID.randomUUID().toString())
                .customerId(orderCreatedEvent.customerId())
                .items(reservedItemEvents)
                .totalAmount(orderCreatedEvent.totalAmount())
                .status(ReservationStatus.RESERVED.name())
                .build();
        applicationEventPublisher.publishEvent(event);
    }

    /**
     * Publica un evento de fallo en la reserva de inventario.
     *
     * @param orderCreatedEvent evento original de la orden creada
     * @param errorMessage      mensaje descriptivo del motivo del fallo
     */
    private void publishFailedInventoryEvent(OrderCreatedEvent orderCreatedEvent, String errorMessage) {
        FailedInventoryEvent failedEvent = FailedInventoryEvent.builder()
                .orderId(orderCreatedEvent.orderId())
                .status(ReservationStatus.FAILED.name())
                .errorMessage(errorMessage)
                .build();
        applicationEventPublisher.publishEvent(failedEvent);
    }
}

package com.cballestas.inventory_service.application.port.in;

import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;

public interface InventoryMessagingPort {
    void handleOrderCreated(OrderCreatedEvent orderCreatedEvent);
    void saveOutboxEvent(String topic, OrderCreatedEvent orderCreatedEvent);
}

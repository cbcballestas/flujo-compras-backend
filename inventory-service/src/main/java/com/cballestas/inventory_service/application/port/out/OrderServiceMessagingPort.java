package com.cballestas.inventory_service.application.port.out;

import com.cballestas.inventory_service.domain.model.event.FailedInventoryEvent;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;

public interface OrderServiceMessagingPort {
    void publishFailedReservedInventoryEvent(FailedInventoryEvent event);
    void publishParkingLotEvent(OrderCreatedEvent event);
}

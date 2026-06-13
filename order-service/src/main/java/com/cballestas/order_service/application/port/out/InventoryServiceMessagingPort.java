package com.cballestas.order_service.application.port.out;

import com.cballestas.order_service.domain.model.dto.response.OrderResponse;

public interface InventoryServiceMessagingPort {
    void publishOrderCreatedEvent(OrderResponse orderResponse);
}

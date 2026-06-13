package com.cballestas.inventory_service.domain.model.event;

import lombok.Builder;

import java.util.List;

@Builder
public record ReservedInventoryEvent(
        String orderId,
        String reservationId,
        String customerId,
        List<ReservedItemEvent> items,
        Double totalAmount,
        String status
) {
}

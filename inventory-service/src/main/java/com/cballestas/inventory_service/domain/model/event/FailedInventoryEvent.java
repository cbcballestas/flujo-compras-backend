package com.cballestas.inventory_service.domain.model.event;

import lombok.Builder;

@Builder
public record FailedInventoryEvent(
        String orderId,
        String status,
        String errorMessage
) {
}

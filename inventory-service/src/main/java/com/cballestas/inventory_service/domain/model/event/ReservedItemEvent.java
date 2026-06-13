package com.cballestas.inventory_service.domain.model.event;

import lombok.Builder;

@Builder
public record ReservedItemEvent(
        String productId,
        String productName,
        Integer quantity,
        Double price
) {
}

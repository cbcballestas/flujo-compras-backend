package com.cballestas.order_service.domain.model.dto.event;

import lombok.Builder;

@Builder
public record OrderItemEvent(
        String productId,
        String productName,
        Integer quantity,
        Double price
) {
}

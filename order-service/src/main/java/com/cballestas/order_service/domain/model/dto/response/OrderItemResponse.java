package com.cballestas.order_service.domain.model.dto.response;

import lombok.Builder;

@Builder
public record OrderItemResponse(
       String productId,
       String productName,
       Integer quantity,
       Double price
) {
}

package com.cballestas.order_service.domain.model.dto.response;

import com.cballestas.order_service.domain.model.enums.OrderStatus;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Builder
public record OrderResponse(
    UUID id,
    String customerId,
    Double totalAmount,
    OrderStatus status,
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    LocalDateTime createdAt,
    List<OrderItemResponse> items
) {
}

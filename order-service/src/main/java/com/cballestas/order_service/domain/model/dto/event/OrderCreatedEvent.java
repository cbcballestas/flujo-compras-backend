package com.cballestas.order_service.domain.model.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;

import java.time.LocalDateTime;
import java.util.List;

@Builder
public record OrderCreatedEvent(
        String orderId,
        String customerId,
        List<OrderItemEvent> items,
        Double totalAmount,
        String status,

        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
        LocalDateTime createdAt,

        int retryCount,
        String errorMessage
) {
}

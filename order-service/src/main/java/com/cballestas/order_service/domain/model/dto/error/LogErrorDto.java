package com.cballestas.order_service.domain.model.dto.error;

import lombok.Builder;

import java.time.LocalDateTime;

@Builder
public record LogErrorDto(
        String topic,
        String errorMessage,
        LocalDateTime timestamp
) {
}

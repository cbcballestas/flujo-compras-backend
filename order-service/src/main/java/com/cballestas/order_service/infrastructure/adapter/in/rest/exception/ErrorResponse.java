package com.cballestas.order_service.infrastructure.adapter.in.rest.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

import java.time.LocalDateTime;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Builder
public record ErrorResponse(
        String status,
        String message,
        String path,
        LocalDateTime timestamp,
        List<ErrorResponseItem> errors
) {
}

package com.cballestas.order_service.infrastructure.adapter.in.rest.exception;

import lombok.Builder;

@Builder
public record ErrorResponseItem(
        String field,
        String error
) {
}

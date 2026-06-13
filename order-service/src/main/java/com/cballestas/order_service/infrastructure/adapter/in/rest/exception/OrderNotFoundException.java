package com.cballestas.order_service.infrastructure.adapter.in.rest.exception;

public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(String message) {
        super(message);
    }
}

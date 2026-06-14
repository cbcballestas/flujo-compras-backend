package com.cballestas.notification_service.infrastructure.in.messaging.exception;

public class BussinessException extends RuntimeException {
    public BussinessException(String message) {
        super(message);
    }
}

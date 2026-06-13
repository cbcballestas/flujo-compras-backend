package com.cballestas.notification_service.infrastructure.out.messaging.exception;

public class MessageException extends RuntimeException {
    public MessageException(String message) {
        super(message);
    }
}

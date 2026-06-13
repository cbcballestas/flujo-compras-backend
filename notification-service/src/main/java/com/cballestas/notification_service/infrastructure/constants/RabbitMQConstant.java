package com.cballestas.notification_service.infrastructure.constants;

public class RabbitMQConstant {
    private RabbitMQConstant() {
        throw new IllegalStateException("Utility class");
    }

    public static final String NOTIFICATION_EXCHANGE_NAME = "exchange-notification";

    public static final String QUEUE_NOTIFICATION_EMAIL = "queue-notification-email"; //  Envíos por email
    public static final String QUEUE_NOTIFICATION_SMS = "queue-notification-sms"; // Envíos por SMS

    public static final String ORDERS_EXCHANGE_NAME = "orders-exchange";
    public static final String ROUTING_ORDER_CONFIRMED_KEY = "route-orders-confirmed";
}

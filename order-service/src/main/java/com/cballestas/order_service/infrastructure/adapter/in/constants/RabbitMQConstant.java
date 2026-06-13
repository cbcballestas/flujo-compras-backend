package com.cballestas.order_service.infrastructure.adapter.in.constants;

public class RabbitMQConstant {
    private RabbitMQConstant() {
        throw new IllegalStateException("Utility class");
    }

    public static final String ORDERS_EXCHANGE_NAME = "orders-exchange";
    public static final String RETRY_ORDERS_EXCHANGE = "orders-exchange-retry";

    public static final String ROUTING_ORDER_CONFIRMED_KEY = "route-orders-confirmed";
    public static final String ROUTING_ORDER_CONFIRMED_RETRY_KEY = "route-orders-confirmed-retry";


    public static final String QUEUE_ORDER_CONFIRMED = "queue-orders-confirmed";
    public static final String QUEUE_ORDER_CONFIRMED_RETRY = "queue-orders-confirmed-retry";

    // DLQ
    public static final String ORDER_DLX_NAME = "exchange-orders-dlx";
    public static final String ROUTING_ORDER_CONFIRMED_DLQ_KEY = "route-orders-confirmed-dlq";
    public static final String QUEUE_ORDER_CONFIRMED_DLQ_NAME = "queue-orders-confirmed-dlq";

    // Parking lot
    public static final String PARKING_LOT_EXCHANGE_NAME ="exchange-orders-confirmed-parking-lot";
    public static final String ROUTING_CONFIRMED_ORDERS_PARKING_LOT_KEY = "route-orders-confirmed-parking-lot";
    public static final String PARKING_LOT_QUEUE_NAME ="queue-orders-confirmed-parking-lot";


}

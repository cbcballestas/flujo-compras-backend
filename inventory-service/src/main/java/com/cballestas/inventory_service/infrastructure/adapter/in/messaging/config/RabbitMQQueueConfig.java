package com.cballestas.inventory_service.infrastructure.adapter.in.messaging.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQQueueConfig {

    public static final String EXCHANGE_NAME = "inventory-exchange";

    public static final String INVENTORY_RESERVED_ORDERS_QUEUE_NAME = "inventory-reserved-orders-queue";
    public static final String INVENTORY_PARKING_LOT_QUEUE_NAME = "inventory-parking-lot-queue";

    /*
     * # : Puede continuar con cualquier palabra o palabras adicionales
     * * : Solo remplaza solo a una palabra
     */

    public static final String ROUTING_RESERVED_ORDERS_KEY = "route.inventory.orders.reserved.#";
    public static final String ROUTING_PARKING_LOT_KEY = "route.inventory.*.parking-lot";

    @Bean
    TopicExchange appExchange() {
        return new TopicExchange(EXCHANGE_NAME,  true, false);
    }

    @Bean
    Queue appQueueInventoryProcessed() {
        return new Queue(INVENTORY_RESERVED_ORDERS_QUEUE_NAME, true);
    }

    @Bean
    Queue appQueueInventoryParkingLot() {
        return new Queue(INVENTORY_PARKING_LOT_QUEUE_NAME, true);
    }

    @Bean
    Binding declareBindingInventoryProcessed(Queue appQueueInventoryProcessed,TopicExchange appExchange ) {
        return BindingBuilder.bind(appQueueInventoryProcessed).to(appExchange).with(ROUTING_RESERVED_ORDERS_KEY);
    }

    @Bean
    Binding declareBindingUserDeleted(Queue appQueueInventoryParkingLot,TopicExchange appExchange) {
        return BindingBuilder.bind(appQueueInventoryParkingLot).to(appExchange).with(ROUTING_PARKING_LOT_KEY);
    }

}

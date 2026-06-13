package com.cballestas.order_service.infrastructure.adapter.in.config;

import com.cballestas.order_service.infrastructure.adapter.in.constants.RabbitMQConstant;
import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQQueueConfig {

    @Bean
    DirectExchange directExchange() {
        return new DirectExchange(RabbitMQConstant.ORDERS_EXCHANGE_NAME);
    }

    @Bean
    DirectExchange retryExchange() {
        return new DirectExchange(RabbitMQConstant.RETRY_ORDERS_EXCHANGE);
    }

    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange(RabbitMQConstant.ORDER_DLX_NAME);
    }

    @Bean
    DirectExchange parkingLotExchange() {
        return new DirectExchange(RabbitMQConstant.PARKING_LOT_EXCHANGE_NAME);
    }

    @Bean
    Queue appQueueOrderConfirmed() {
        return QueueBuilder.durable(RabbitMQConstant.QUEUE_ORDER_CONFIRMED)
                .withArgument("x-dead-letter-exchange", RabbitMQConstant.RETRY_ORDERS_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", RabbitMQConstant.ROUTING_ORDER_CONFIRMED_RETRY_KEY)
                .build();
    }

    @Bean
    Queue appQueueOrderConfirmedRetry() {
        return QueueBuilder.durable(RabbitMQConstant.QUEUE_ORDER_CONFIRMED_RETRY)
                .withArgument("x-message-ttl", 5000)
                .withArgument("x-dead-letter-exchange", RabbitMQConstant.ORDERS_EXCHANGE_NAME)
                .withArgument("x-dead-letter-routing-key", RabbitMQConstant.ROUTING_ORDER_CONFIRMED_KEY)
                .build();
    }

    @Bean
    Queue queueUserDLQ() {
        return QueueBuilder.durable(RabbitMQConstant.QUEUE_ORDER_CONFIRMED_DLQ_NAME).build();
    }

    @Bean
    Queue queueUserParkingLot() {
        return QueueBuilder
                .durable(RabbitMQConstant.PARKING_LOT_QUEUE_NAME)
                .build();
    }

    @Bean
    Binding declareBindingOrderConfirmed(Queue appQueueOrderConfirmed, DirectExchange directExchange) {
        return BindingBuilder.bind(appQueueOrderConfirmed).to(directExchange).with(RabbitMQConstant.ROUTING_ORDER_CONFIRMED_KEY);
    }

    @Bean
    Binding declareBindingQueueUserDLQ(
            Queue queueUserDLQ,
            DirectExchange deadLetterExchange) {

        return BindingBuilder.bind(queueUserDLQ)
                .to(deadLetterExchange)
                .with(RabbitMQConstant.ROUTING_ORDER_CONFIRMED_DLQ_KEY);
    }

    @Bean
    Binding declareBindingQueueUserNotificationRetry(
            Queue appQueueOrderConfirmedRetry,
            DirectExchange retryExchange) {

        return BindingBuilder.bind(appQueueOrderConfirmedRetry)
                .to(retryExchange)
                .with(RabbitMQConstant.ROUTING_ORDER_CONFIRMED_RETRY_KEY);
    }

    @Bean
    Binding declareBindingQueueUserParkingLot(
            Queue queueUserParkingLot,
            DirectExchange parkingLotExchange) {

        return BindingBuilder.bind(queueUserParkingLot)
                .to(parkingLotExchange)
                .with(RabbitMQConstant.ROUTING_CONFIRMED_ORDERS_PARKING_LOT_KEY);
    }

}

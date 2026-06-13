package com.cballestas.notification_service.infrastructure.config;

import com.cballestas.notification_service.infrastructure.constants.RabbitMQConstant;
import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQQueueConfig {

    @Bean
    FanoutExchange appExchange() {
        return new FanoutExchange(RabbitMQConstant.NOTIFICATION_EXCHANGE_NAME);
    }

    @Bean
    DirectExchange ordersExchange() {
        return new DirectExchange(RabbitMQConstant.ORDERS_EXCHANGE_NAME);
    }

    @Bean
    Queue appQueueNotificationEmail() {
        return new Queue(RabbitMQConstant.QUEUE_NOTIFICATION_EMAIL, true);
    }

    @Bean
    Queue appQueueNotificationSms() {
        return new Queue(RabbitMQConstant.QUEUE_NOTIFICATION_SMS, true);
    }

    @Bean
    Binding declareBindingNotificationEmail(Queue appQueueNotificationEmail, FanoutExchange appExchange) {
        return BindingBuilder.bind(appQueueNotificationEmail).to(appExchange);
    }

    @Bean
    Binding declareBindingNotificationSms(Queue appQueueNotificationSms, FanoutExchange appExchange) {
        return BindingBuilder.bind(appQueueNotificationSms).to(appExchange);
    }
}

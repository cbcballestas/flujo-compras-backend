package com.cballestas.notification_service.infrastructure.out.messaging.listener;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.notification_service.application.ports.out.EmailServicePort;
import com.cballestas.notification_service.infrastructure.constants.RabbitMQConstant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
@Slf4j
public class NotificationSenderListener {

    private final EmailServicePort emailServicePort;

    @RabbitListener(queues = RabbitMQConstant.QUEUE_NOTIFICATION_EMAIL)
    public void processEmailNotification(ReservedInventoryEvent event) {
        log.info("Processing email notification to customer {}", event.customerId());
        emailServicePort.sendEmail(event);
    }

    @RabbitListener(queues = RabbitMQConstant.QUEUE_NOTIFICATION_SMS)
    public void processSmsNotification(ReservedInventoryEvent event) {
        log.info("Processing SMS notification");
        log.info("Simulating SMS sending for order {} to customer {}", event.orderId(), event.customerId());
        log.info("SMS sent successfully for order {} to customer {}", event.orderId(), event.customerId());
    }
}

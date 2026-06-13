package com.cballestas.notification_service.infrastructure.out.messaging.service.impl;

import com.cballestas.notification_service.application.ports.out.NotificationPersistencePort;
import com.cballestas.notification_service.application.ports.out.OutboxEventPersistencePort;
import com.cballestas.notification_service.domain.model.OutBoxEvent;
import com.cballestas.notification_service.infrastructure.constants.RabbitMQConstant;
import com.cballestas.notification_service.infrastructure.out.messaging.dto.FailedNotificationEvent;
import com.cballestas.notification_service.infrastructure.out.messaging.dto.UpdateNotificationStatusEvent;
import com.cballestas.notification_service.infrastructure.out.messaging.service.NotificationMessageService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationMessageServiceImpl implements NotificationMessageService {

    private final NotificationPersistencePort notificationPersistencePort;
    private final OutboxEventPersistencePort outboxEventPersistencePort;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Nombre del topic Kafka para eventos de inventario reservado, inyectado desde propiedades.
     */
    @Value("${app.kafka.topics.inventory-reserved}")
    private String inventoryReservedTopic;

    @Transactional
    @Override
    public void saveParkingLot(FailedNotificationEvent failedNotificationEvent) {
        try {
            var reservedInventoryEvent = failedNotificationEvent.reservedInventoryEvent();

            String reservedInventoryJson = objectMapper.writeValueAsString(reservedInventoryEvent);
            log.info("Parking lot event received: {}", reservedInventoryJson);

            var notificationOutBoxEvent = OutBoxEvent.builder()
                    .key(reservedInventoryEvent.orderId())
                    .topic(inventoryReservedTopic)
                    .payload(reservedInventoryJson)
                    .published(Boolean.FALSE)
                    .build();
            log.info("Saving notification to database: {}", notificationOutBoxEvent);
            outboxEventPersistencePort.save(notificationOutBoxEvent);
        } catch (JsonProcessingException e) {
            log.error("Error while saving notification to database: {}", e.getMessage());
        }
    }

    @Transactional
    @Override
    public void updateNotificationStatus(UpdateNotificationStatusEvent updateNotificationStatusEvent) {
        log.info("Updating notification status for order: {}, reservation: {}, customer: {}, new status: {}",
                updateNotificationStatusEvent.orderId(),
                updateNotificationStatusEvent.reservationId(),
                updateNotificationStatusEvent.customerId(),
                updateNotificationStatusEvent.status());

        // Actualizar el estado de la notificación a SENT (enviada)
        var savedNotification = notificationPersistencePort.findByOrderIdAndReservationIdAndCustomerId(
                updateNotificationStatusEvent.orderId(), updateNotificationStatusEvent.reservationId(), updateNotificationStatusEvent.customerId())
                .orElseThrow(() -> new RuntimeException("Notification not found after saving"));

        savedNotification.setStatus(updateNotificationStatusEvent.status());
        savedNotification.setMessage(String.format("Notification sent for order %s", updateNotificationStatusEvent.orderId()));
        notificationPersistencePort.save(savedNotification);

        log.info("Notification sent successfully for order: {}", updateNotificationStatusEvent.orderId());

        // Enviar mensaje de confirmación a servicio de ordenes para actualizar el estado de la orden
        rabbitTemplate.convertAndSend(RabbitMQConstant.ORDERS_EXCHANGE_NAME, RabbitMQConstant.ROUTING_ORDER_CONFIRMED_KEY, updateNotificationStatusEvent.orderId());
    }
}

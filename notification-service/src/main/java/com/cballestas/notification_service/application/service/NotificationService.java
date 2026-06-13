package com.cballestas.notification_service.application.service;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.notification_service.application.ports.in.NotificationServicePort;
import com.cballestas.notification_service.application.ports.out.EmailServicePort;
import com.cballestas.notification_service.application.ports.out.NotificationPersistencePort;
import com.cballestas.notification_service.domain.enums.NotificationStatus;
import com.cballestas.notification_service.domain.model.Notification;
import com.cballestas.notification_service.infrastructure.constants.RabbitMQConstant;
import com.cballestas.notification_service.infrastructure.out.messaging.dto.UpdateNotificationStatusEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService implements NotificationServicePort {

    private final NotificationPersistencePort notificationPersistencePort;
    private final RabbitTemplate rabbitTemplate;
    private final ApplicationEventPublisher applicationEventPublisher;

    /**
     * Procesa el envío de una notificación para un evento de inventario reservado.
     * Persiste la notificación, envía el correo al cliente y actualiza el estado a SENT.
     *
     * @param event evento {@link ReservedInventoryEvent} con los datos de la orden reservada
     * @throws RuntimeException si la notificación no se encuentra después de guardar o si ocurre un error en el flujo
     */
    @Override
    public void send(ReservedInventoryEvent event) {
        log.info("Processing notification for order: {}", event.orderId());

        // Persistir la notificación en la base de datos
        var notification = Notification.builder()
                .orderId(event.orderId())
                .reservationId(event.reservationId())
                .customerId(event.customerId())
                .message(String.format("Order %s has been reserved successfully!", event.orderId()))
                .totalAmount(event.totalAmount())
                .status(NotificationStatus.PENDING)
                .build();

        notificationPersistencePort.save(notification);

        // Enviar notificaciones por email y sms
        rabbitTemplate.convertAndSend(RabbitMQConstant.NOTIFICATION_EXCHANGE_NAME, "", event);

        applicationEventPublisher.publishEvent(new UpdateNotificationStatusEvent(event.orderId(), event.reservationId(), event.customerId(), NotificationStatus.SENT));
    }
}

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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationMessageServiceImpl implements NotificationMessageService {

    private final NotificationPersistencePort notificationPersistencePort;
    private final OutboxEventPersistencePort outboxEventPersistencePort;

    private final RabbitTemplate rabbitTemplate;

    @Qualifier("kafkaTemplate")
    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    /**
     * Nombre del topic Kafka para eventos de inventario reservado, inyectado desde propiedades.
     */
    @Value("${app.kafka.topics.inventory-reserved}")
    private String inventoryReservedTopic;

    @Value("${app.kafka.topics.inventory-reservation-failed}")
    private String inventoryReservationFailedTopic;

    /**
     * Guarda un evento de notificación fallida en la tabla de parking lot (outbox) para reprocesamiento posterior.
     * Este método almacena el evento en la base de datos para garantizar que no se pierda en caso de fallos
     * en la entrega a través de Kafka.
     *
     * @param failedNotificationEvent evento de notificación fallida que contiene la información del inventario
     *                                no reservado y los detalles asociados
     * @throws RuntimeException si ocurre un error durante la serialización JSON del evento
     */
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
                    .errorMessage(failedNotificationEvent.errorMessage())
                    .published(Boolean.FALSE)
                    .build();

            publishToInventoryReservationFailedTopic(notificationOutBoxEvent);

            log.info("Saving notification to database: {}", notificationOutBoxEvent);
            outboxEventPersistencePort.save(notificationOutBoxEvent);
        } catch (JsonProcessingException e) {
            log.error("Error while saving notification to database: {}", e.getMessage());
        }
    }

    /**
     * Actualiza el estado de una notificación a SENT (enviada) y envía un mensaje de confirmación
     * al servicio de órdenes mediante RabbitMQ para notificar que la notificación fue procesada exitosamente.
     *
     * @param updateNotificationStatusEvent evento que contiene el identificador de la orden, reserva, cliente
     *                                      y el nuevo estado de la notificación
     * @throws RuntimeException si la notificación no se encuentra en la base de datos después de haber sido guardada
     */
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

    /**
     * Publica un evento de falló de reserva de inventario al topic de Kafka
     * {@link #inventoryReservationFailedTopic}. El evento es serializado a JSON y se envía con la
     * clave proporcionada desde el evento de outbox.
     *
     * @param outBoxEvent evento de outbox que contiene la información del evento fallido, incluyendo
     *                    la clave y el payload a serializar
     * @throws RuntimeException si ocurre un error durante la serialización JSON del evento al publicar
     */
    private void publishToInventoryReservationFailedTopic(OutBoxEvent outBoxEvent) {
        try {
            String failedInventoryReservationJson = objectMapper.writeValueAsString(outBoxEvent);
            log.info("Publishing failed inventory reservation event to Kafka topic {}: {}", inventoryReservationFailedTopic, failedInventoryReservationJson);
            kafkaTemplate.send(inventoryReservationFailedTopic, outBoxEvent.getKey(), failedInventoryReservationJson);
        } catch (JsonProcessingException e) {
            log.error("Error while publishing failed inventory reservation event to Kafka: {}", e.getMessage());
        }
    }

}

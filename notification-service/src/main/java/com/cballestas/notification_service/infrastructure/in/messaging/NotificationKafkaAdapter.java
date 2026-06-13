package com.cballestas.notification_service.infrastructure.in.messaging;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.notification_service.application.ports.in.NotificationServicePort;
import com.cballestas.notification_service.infrastructure.out.messaging.dto.FailedNotificationEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.MessagingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationKafkaAdapter {

    private final NotificationServicePort notificationServicePort;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.inventory-reserved}")
    private String inventoryReservedTopic;

    @RetryableTopic(
            listenerContainerFactory = "reservedInventoryListenerContainerFactory",
            include = {
                    KafkaException.class,
                    KafkaProducerException.class,
                    MessagingException.class
            },
            backoff = @Backoff(delay = 3000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = "-try", dltTopicSuffix = "-dlt")

    @KafkaListener(topics = {"${app.kafka.topics.inventory-reserved}"},
            groupId = "${app.kafka.notification-group-id}",
            containerFactory = "reservedInventoryListenerContainerFactory")
    void consumeReservedInventoryEvent(ReservedInventoryEvent reservedInventoryEvent) {
        log.info("Received reserved inventory event {}", reservedInventoryEvent);
        notificationServicePort.send(reservedInventoryEvent);
    }

    @DltHandler
    public void handleDltOrderCreatedEvent(
            ReservedInventoryEvent reservedInventoryEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = "nroRetry", required = false) String nroRetry) {
        log.info("Retries attempts: {}", nroRetry);
        int iNroRetry = ((nroRetry == null) ? 0 : Integer.parseInt(nroRetry));
        log.info("Message dlt topic={}, payload={}, exceptionMessage={}", topic, reservedInventoryEvent, exceptionMessage);

        if (iNroRetry < 3) {
            try {
                String eventJson = objectMapper.writeValueAsString(reservedInventoryEvent);
                Map<String, Object> headers = Map.of(
                        KafkaHeaders.TOPIC, inventoryReservedTopic,
                        KafkaHeaders.KEY, reservedInventoryEvent.orderId(),
                        "nroRetry", String.valueOf(iNroRetry + 1),
                        "contentType", "application/json");
                kafkaTemplate.send(new GenericMessage<>(eventJson, new MessageHeaders(headers)));
            } catch (JsonProcessingException e) {
                log.error("Error serializing ReservedInventoryEvent for retry: {}", e.getMessage(), e);
                applicationEventPublisher.publishEvent(new FailedNotificationEvent(reservedInventoryEvent));
            }
        } else {
            // Database Persiste(NO_SQL - MongoDb/ Casandra) or File Serve
            applicationEventPublisher.publishEvent(new FailedNotificationEvent(reservedInventoryEvent));
        }
    }
}

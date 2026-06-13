package com.cballestas.inventory_service.infrastructure.adapter.out.messaging;

import com.cballestas.inventory_service.application.port.out.NotificationServiceMessagingPort;
import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.order_service.domain.model.dto.error.LogErrorDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationServiceMessagingAdapter implements NotificationServiceMessagingPort {

    private final KafkaProducer<String, ReservedInventoryEvent> producer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Topicos
    @Value("${app.kafka.topics.inventory-reserved}")
    private String inventoryReservedTopic;

    @Value("${app.kafka.topics.log-error}")
    private String inventoryLogErrorTopic;

    @Override
    public void publishReservedInventoryEvent(ReservedInventoryEvent event) {
        log.info("Publishing Reserved Inventory Event: {}", event);

        // Utilizamos el ID de la orden como clave para garantizar que los eventos relacionados con la misma orden se envíen al mismo partición
        var producerRecord = new ProducerRecord<>(inventoryReservedTopic, event.orderId(), event);

        // Enviamos el mensaje de forma asíncrona y registramos el resultado en el log
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                log.info("Reserved inventory event published: orderId={}, topic={}, partition={}",
                        event.orderId(), metadata.topic(), metadata.partition());
            } else {
                log.error("Failed to publish reserved inventory event: orderId={}, topic={}, error={}",
                        event.orderId(), inventoryReservedTopic, exception.getMessage());
                log.error("Event Message: {}", event);

                var logErrorDto = LogErrorDto.builder()
                        .topic(inventoryReservedTopic)
                        .errorMessage(exception.getMessage())
                        .timestamp(LocalDateTime.now())
                        .build();

                try {
                    String logErrorJson = objectMapper.writeValueAsString(logErrorDto);
                    kafkaTemplate.send(inventoryLogErrorTopic, logErrorJson);
                    log.info("Error log sent to topic {}: {}", inventoryLogErrorTopic, logErrorJson);
                } catch (JsonProcessingException e) {
                    log.error("JsonProcessingException => {}", e.getMessage(), e);
                }

            }
        });
    }
}

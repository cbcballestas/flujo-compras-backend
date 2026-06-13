package com.cballestas.order_service.infrastructure.adapter.out.messaging;

import com.cballestas.order_service.application.port.out.InventoryServiceMessagingPort;
import com.cballestas.order_service.domain.model.dto.error.LogErrorDto;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;
import com.cballestas.order_service.domain.model.dto.response.OrderResponse;
import com.cballestas.order_service.infrastructure.adapter.out.messaging.mapper.OrderEventMapper;
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
public class InventoryServiceMessagingAdapter implements InventoryServiceMessagingPort {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProducer<String, OrderCreatedEvent> producer;
    private final ObjectMapper objectMapper;
    private final OrderEventMapper orderEventMapper;

    @Value("${app.kafka.topics.orders-created}")
    private String ordersCreatedTopic;

    @Value("${app.kafka.topics.orders-confirmed}")
    private String ordersConfirmedTopic;

    @Value("${app.kafka.topics.orders-cancelled}")
    private String ordersCancelledTopic;

    @Value("${app.kafka.topics.orders-completed}")
    private String ordersCompletedTopic;

    @Value("${app.kafka.topics.log-error}")
    private String ordersErrorsTopic;

    /**
     * Publica un evento de creación de orden en el tópico de Kafka correspondiente.
     * <p>
     * Este método transforma la respuesta de la orden en un evento de dominio y lo envía al tópico de Kafka
     * configurado para órdenes creadas. Utiliza el identificador de la orden como clave del mensaje. El envío es
     * asíncrono y registra en el log si la publicación fue exitosa o si ocurrió un error.
     *
     * @param orderResponse la respuesta de la orden que será transformada y publicada como evento
     */
    @Override
    public void publishOrderCreatedEvent(OrderResponse orderResponse) {
        var orderEvent = orderEventMapper.toEvent(orderResponse);

        // Utilizamos el ID de la orden como clave para garantizar que los eventos relacionados con la misma orden se envíen al mismo partición
        var producerRecord = new ProducerRecord<>(ordersCreatedTopic, orderEvent.orderId(), orderEvent);

        // Enviamos el mensaje de forma asíncrona y registramos el resultado en el log
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                log.info("Order created event published: orderId={}, topic={}, partition={}",
                        orderResponse.id(), metadata.topic(), metadata.partition());
            } else {
                log.error("Failed to publish order created event: orderId={}, topic={}, error={}",
                        orderResponse.id(), ordersCreatedTopic, exception.getMessage());
                log.error("Message: {}", orderEvent);

                LogErrorDto logErrorDto = LogErrorDto.builder()
                        .topic(ordersCreatedTopic)
                        .errorMessage(exception.getMessage())
                        .timestamp(LocalDateTime.now())
                        .build();

                try {
                    String logErrorJson = objectMapper.writeValueAsString(logErrorDto);
                    kafkaTemplate.send(ordersErrorsTopic, logErrorJson);
                    log.info("Error log sent to topic {}: {}", ordersErrorsTopic, logErrorJson);
                } catch (JsonProcessingException e) {
                    log.error("JsonProcessingException => {}", e.getMessage(), e);
                }

            }
        });
    }
}

package com.cballestas.notification_service.infrastructure.in.messaging;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.notification_service.application.ports.in.NotificationServicePort;
import com.cballestas.notification_service.infrastructure.in.messaging.exception.BussinessException;
import com.cballestas.notification_service.infrastructure.out.messaging.dto.FailedNotificationEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.MessagingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationKafkaAdapter {

    private final NotificationServicePort notificationServicePort;
    private final ApplicationEventPublisher applicationEventPublisher;
    @Qualifier("reservedInventoryKafkaTemplate")
    private final KafkaTemplate<String, ReservedInventoryEvent> reservedInventoryTemplate;
    private final ObjectMapper objectMapper;

    // Constantes para validación de reintentos
    private static final int MAX_RETRY_ATTEMPTS = 2;
    private static final String RETRY_HEADER = "nroRetry";

    @Value("${app.kafka.topics.inventory-reserved}")
    private String inventoryReservedTopic;

    /**
     * Consume eventos de inventario reservado del tópico Kafka configurado.
     * Valida completamente el evento y lo envía al servicio de notificaciones.
     * En caso de error, reininta automáticamente según la configuración de {@link RetryableTopic}.
     *
     * @param reservedInventoryEvent evento de inventario reservado consumido de Kafka
     * @throws BussinessException si la validación del evento falla
     * @throws MessagingException si ocurre un error al procesar el mensaje
     */
    @RetryableTopic(
            listenerContainerFactory = "reservedInventoryListenerContainerFactory",
            include = {
                    MessagingException.class,
                    BussinessException.class
            },
            backoff = @Backoff(delay = 3000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = "-try", dltTopicSuffix = "-dlt")

    @KafkaListener(topics = {"${app.kafka.topics.inventory-reserved}"},
            groupId = "${app.kafka.notification-group-id}",
            containerFactory = "reservedInventoryListenerContainerFactory")
    void consumeReservedInventoryEvent(ReservedInventoryEvent reservedInventoryEvent) {
        log.info("Received reserved inventory event {}", reservedInventoryEvent);
        validateReservedInventoryEvent(reservedInventoryEvent);
        notificationServicePort.send(reservedInventoryEvent);
    }

    /**
     * Valida que el evento ReservedInventoryEvent contenga todos los campos requeridos
     * y que los valores sean válidos según las reglas de negocio.
     *
     * @param event el evento a validar
     * @throws BussinessException si falta algún campo requerido o los valores no son válidos
     */
    private void validateReservedInventoryEvent(ReservedInventoryEvent event) {
        validateEventNotNull(event);
        validateRequiredStringFields(event);
        validateMonetaryFields(event);
        validateItemsList(event);
        log.debug("ReservedInventoryEvent validado correctamente: orderId={}", event.orderId());
    }

    /**
     * Valida que el evento no sea nulo.
     *
     * @param event el evento a verificar
     * @throws BussinessException si el evento es nulo
     */
    private void validateEventNotNull(ReservedInventoryEvent event) {
        if (event == null) {
            throw new BussinessException("ReservedInventoryEvent no puede ser null");
        }
    }

    /**
     * Valida todos los campos de tipo string requeridos del evento de inventario reservado.
     *
     * @param event el evento cuyo campos string se validarán
     * @throws BussinessException si alguno de los campos requeridos está vacío o es nulo
     */
    private void validateRequiredStringFields(ReservedInventoryEvent event) {
        validateRequiredField(event.orderId(), "orderId");
        validateRequiredField(event.reservationId(), "reservationId");
        validateRequiredField(event.customerId(), "customerId");
        validateRequiredField(event.status(), "status");
    }

    /**
     * Valida los campos monetarios del evento, asegurando que existan y sean positivos.
     *
     * @param event el evento cuyo monto total se validará
     * @throws BussinessException si el monto es nulo, cero o negativo
     */
    private void validateMonetaryFields(ReservedInventoryEvent event) {
        if (event.totalAmount() == null) {
            throw new BussinessException("El campo 'totalAmount' es requerido");
        }

        if (event.totalAmount() <= 0) {
            throw new BussinessException("El campo 'totalAmount' debe ser mayor a cero");
        }
    }

    /**
     * Valida la lista de items del evento, verificando que no esté vacía
     * y que cada item contenga datos válidos.
     *
     * @param event el evento cuya lista de items será validada
     * @throws BussinessException si la lista está nula, vacía o contiene items inválidos
     */
    private void validateItemsList(ReservedInventoryEvent event) {
        if (event.items() == null || event.items().isEmpty()) {
            throw new BussinessException("El campo 'items' es requerido y debe contener al menos un elemento");
        }

        for (int i = 0; i < event.items().size(); i++) {
            validateItem(event.items().get(i), i);
        }
    }

    /**
     * Valida los datos de un item individual de la reserva de inventario.
     *
     * @param itemObj el objeto item a validar
     * @param index   la posición del item en la lista (usado para mensajes de error descriptivos)
     * @throws BussinessException si el item es nulo o contiene campos inválidos
     */
    private void validateItem(Object itemObj, int index) {
        if (itemObj == null) {
            throw new BussinessException("Item en posición " + index + " no puede ser null");
        }

        var item = (com.cballestas.inventory_service.domain.model.event.ReservedItemEvent) itemObj;
        String itemPrefix = "Item en posición " + index;

        validateRequiredField(item.productId(), itemPrefix + " - productId");
        validateRequiredField(item.productName(), itemPrefix + " - productName");
        validatePositiveQuantity(item.quantity(), itemPrefix);
        validateNonNegativePrice(item.price(), itemPrefix);
    }

    /**
     * Valida que un campo de tipo string sea requerido y no esté vacío.
     *
     * @param fieldValue el valor del campo a validar
     * @param fieldName  nombre del campo (para mensajes de error descriptivos)
     * @throws BussinessException si el campo es nulo o está vacío
     */
    private void validateRequiredField(String fieldValue, String fieldName) {
        if (fieldValue == null || fieldValue.trim().isEmpty()) {
            throw new BussinessException("El campo '" + fieldName + "' es requerido y no puede estar vacío");
        }
    }

    /**
     * Valida que una cantidad sea un número positivo.
     *
     * @param quantity    la cantidad a validar
     * @param itemPrefix  prefijo descriptivo del contexto (para mensajes de error)
     * @throws BussinessException si la cantidad es nula, cero o negativa
     */
    private void validatePositiveQuantity(Integer quantity, String itemPrefix) {
        if (quantity == null) {
            throw new BussinessException(itemPrefix + ": el campo 'quantity' es requerido");
        }

        if (quantity <= 0) {
            throw new BussinessException(itemPrefix + ": el campo 'quantity' debe ser mayor a cero");
        }
    }

    /**
     * Valida que un precio sea un número no negativo.
     *
     * @param price       el precio a validar
     * @param itemPrefix  prefijo descriptivo del contexto (para mensajes de error)
     * @throws BussinessException si el precio es nulo o negativo
     */
    private void validateNonNegativePrice(Double price, String itemPrefix) {
        if (price == null) {
            throw new BussinessException(itemPrefix + ": el campo 'price' es requerido");
        }

        if (price < 0) {
            throw new BussinessException(itemPrefix + ": el campo 'price' no puede ser negativo");
        }
    }

    /**
     * Maneja los mensajes que han fallado después de los reintentos automáticos.
     * Realiza validación de reintentos y reenvía al tópico original si no se ha superado el límite.
     * Si se agota el límite de reintentos, publica un evento de notificación fallida para
     * registro y auditoría.
     *
     * @param reservedInventoryEvent evento de inventario reservado que falló en todos los reintentos
     * @param topic                 nombre del tópico DLT donde se recibió el mensaje
     * @param exceptionMessage      mensaje de excepción que causó el último fallo
     * @param headers               encabezados del mensaje Kafka con metadatos adicionales
     * @param nroRetry             contador de reintentos (puede ser nulo en el primer intento de DLT)
     */
    @DltHandler
    public void handleDltReservedInventoryEvent(
            ReservedInventoryEvent reservedInventoryEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Headers MessageHeaders headers,
            @Header(value = RETRY_HEADER, required = false) String nroRetry) {

        // ✅ Validar y parsear número de reintentos
        int currentRetryAttempt = validateAndParseRetryAttempt(nroRetry);

        log.info("🔄 DLT Handler activado - Topic: {}, Retries attempts: {}/{}",
                topic, currentRetryAttempt, MAX_RETRY_ATTEMPTS);
        log.error("❌ Message en DLT - orderId={}, exceptionMessage={}",
                reservedInventoryEvent.orderId(), exceptionMessage);

        // ✅ Validar si aún hay reintentos disponibles
        if (canRetry(currentRetryAttempt)) {
            performRetry(reservedInventoryEvent, currentRetryAttempt, topic, exceptionMessage);
        } else {
            // ✅ Se agotaron reintentos: publicar evento de fallo
            publishFailureEvent(reservedInventoryEvent, currentRetryAttempt);
        }
    }

    /**
     * Valida y parsea el número de reintentos desde el encabezado del mensaje Kafka.
     * Retorna 0 si el encabezado es nulo o inválido.
     *
     * @param nroRetry valor del encabezado de reintentos (puede ser nulo o vacío)
     * @return número entero de intentos realizados, 0 si el valor es inválido
     */
    private int validateAndParseRetryAttempt(String nroRetry) {
        if (nroRetry == null || nroRetry.trim().isEmpty()) {
            log.debug("Primer reintento: nroRetry es nulo");
            return 0;
        }

        try {
            int attempt = Integer.parseInt(nroRetry);
            if (attempt < 0) {
                log.warn("⚠️ nroRetry negativo detectado: {}. Usando 0.", attempt);
                return 0;
            }
            return attempt;
        } catch (NumberFormatException e) {
            log.error("⚠️ nroRetry inválido (no es número): '{}'. Usando 0.", nroRetry);
            return 0;
        }
    }

    /**
     * Determina si se pueden realizar más reintentos basándose en el número actual de intentos
     * y el límite máximo configurado.
     *
     * @param currentRetryAttempt número actual de intentos realizados
     * @return {@code true} si se pueden hacer más reintentos, {@code false} si se agotó el límite
     */
    private boolean canRetry(int currentRetryAttempt) {
        int realCurrentAttempt = currentRetryAttempt + 1;
        boolean canRetry = realCurrentAttempt < MAX_RETRY_ATTEMPTS;
        log.debug("Validación de reintentos: intento actual={}, máximo={}, ¿puede reintentar?={}",
                realCurrentAttempt, MAX_RETRY_ATTEMPTS, canRetry);
        return canRetry;
    }

    /**
     * Realiza el reintento enviando el evento al tópico original,
     * incrementando el contador de reintentos en el encabezado del mensaje.
     *
     * @param event             evento a reintentar
     * @param currentAttempt    número actual de intentos realizados
     * @param topic             nombre del tópico al que se reenviará el mensaje
     * @param exceptionMessage  mensaje de excepción que causó el fallo anterior
     */
    private void performRetry(ReservedInventoryEvent event, int currentAttempt, String topic, String exceptionMessage) {
        int nextAttempt = currentAttempt + 1;
        log.info("📤 Reenviando mensaje al tópico original. Intento {}/{}", nextAttempt, MAX_RETRY_ATTEMPTS);

        try {
            // ✅ Enviar con template tipado y handler de callbacks

            Message<ReservedInventoryEvent> message = MessageBuilder
                    .withPayload(event)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader(RETRY_HEADER, nextAttempt)
                    .setHeader(KafkaHeaders.EXCEPTION_MESSAGE, exceptionMessage)
                    .setHeader("contentType", "application/json") // Útil para tu configuración de DLT
                    .build();

            reservedInventoryTemplate.send(message)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("❌ Error enviando mensaje a Kafka en reintento #{}: {}",
                                    nextAttempt, ex.getMessage(), ex);
                        } else {
                            log.info("✅ Mensaje reenviado exitosamente. Intento #{} de {}",
                                    nextAttempt, MAX_RETRY_ATTEMPTS);
                        }
                    });
        } catch (Exception e) {
            log.error("❌ Excepción crítica en performRetry: {}", e.getMessage(), e);
            publishFailureEvent(event, nextAttempt);
        }
    }

    /**
     * Publica un evento de notificación fallida cuando se agota el límite de reintentos
     * o cuando falla el envío durante el reintento. Este evento se publica en el contexto
     * de aplicación para ser procesado por otros listeners del servicio.
     *
     * @param event         evento de inventario reservado que falló finalmente
     * @param attemptCount  número total de intentos realizados
     */
    private void publishFailureEvent(ReservedInventoryEvent event, int attemptCount) {
        log.error("🛑 Descartando mensaje después de {} reintentos. orderId={}", attemptCount, event.orderId());
        try {
            FailedNotificationEvent failureEvent = new FailedNotificationEvent(event);
            applicationEventPublisher.publishEvent(failureEvent);
            log.info("✅ FailedNotificationEvent publicado para orderId={}", event.orderId());
        } catch (Exception e) {
            log.error("❌ Error publicando FailedNotificationEvent: {}", e.getMessage(), e);
        }
    }
}

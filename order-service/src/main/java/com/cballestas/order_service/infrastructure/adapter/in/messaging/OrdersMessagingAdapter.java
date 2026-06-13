package com.cballestas.order_service.infrastructure.adapter.in.messaging;

import com.cballestas.order_service.application.port.out.OrderPersistencePort;
import com.cballestas.order_service.domain.model.enums.OrderStatus;
import com.cballestas.order_service.infrastructure.adapter.in.constants.RabbitMQConstant;
import com.cballestas.order_service.infrastructure.adapter.in.exception.MessageException;
import com.cballestas.order_service.infrastructure.adapter.in.rest.exception.OrderNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrdersMessagingAdapter {

    private final OrderPersistencePort orderPersistencePort;
    private final RabbitTemplate rabbitTemplate;

    @RabbitListener(queues = RabbitMQConstant.QUEUE_ORDER_CONFIRMED)
    @Transactional
    public void updateConfirmedOrderStatus(String orderId, Message message) {
        try {
            log.info("====================================");
            log.info("Mensaje recibido {}", LocalDateTime.now());
            log.info("ID de orden a confirmar: {}", orderId);
            log.info("====================================");
            var order = orderPersistencePort.findById(orderId)
                    .orElseThrow(() -> new OrderNotFoundException("Order not found with id: " + orderId));
            order.setStatus(OrderStatus.CONFIRMED);
            orderPersistencePort.save(order);
            log.info("Orden {} ha sido confirmada", orderId);
        } catch (Exception e) {
            long retries = getRetryCount(message);

            log.warn("Intento {} fallido. Motivo: {}",(retries + 1),e.getMessage());

            if (retries >= 2) {

                log.warn("Máximo de reintentos alcanzado. Enviando a Parking Lot. Intentos: {}, mensaje: {}",retries + 1,orderId);

                rabbitTemplate.convertAndSend(
                        RabbitMQConstant.PARKING_LOT_EXCHANGE_NAME,
                        RabbitMQConstant.ROUTING_CONFIRMED_ORDERS_PARKING_LOT_KEY,
                        orderId
                );

                log.info("Mensaje enviado correctamente a Parking Lot");
                return;
            }

            throw new MessageException("Error procesando confirmación de orden "+ orderId, e);
        }
    }

    @SuppressWarnings("unchecked")
    private long getRetryCount(Message message) {

        Object xDeathHeader = message.getMessageProperties()
                .getHeaders()
                .get("x-death");

        if (!(xDeathHeader instanceof java.util.List<?> xDeathList) || xDeathList.isEmpty()) {
            return 0;
        }

        java.util.Map<String, Object> xDeath =
                (java.util.Map<String, Object>) xDeathList.getFirst();

        Object count = xDeath.get("count");

        if (count instanceof Long value) {
            return value;
        }

        return 0;
    }
}

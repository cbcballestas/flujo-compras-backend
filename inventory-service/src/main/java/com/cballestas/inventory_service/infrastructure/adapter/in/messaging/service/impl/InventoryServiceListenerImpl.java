package com.cballestas.inventory_service.infrastructure.adapter.in.messaging.service.impl;

import com.cballestas.inventory_service.domain.model.event.OutBoxEvent;
import com.cballestas.inventory_service.infrastructure.adapter.in.messaging.service.InventoryListenerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryServiceListenerImpl implements InventoryListenerService {

    private final RabbitTemplate rabbitTemplate;

    private static final String PARKING_LOT_KEY = "route.inventory.parking-lot.";
    private static final String RESERVED_ORDER_KEY = "route.inventory.orders-reserved";

    /**
     * Envía un evento de OutBox a la cola de estacionamiento (Parking Lot) para su procesamiento posterior.
     * <p>
     * Este método es invocado cuando ocurre un error durante el procesamiento de eventos y el evento
     * necesita ser almacenado temporalmente para reintentos posteriores o auditoría.
     * </p>
     *
     * @param event el evento {@link OutBoxEvent} que será enviado a la cola de estacionamiento
     */
    @Override
    public void handleBackupOutboxEvent(OutBoxEvent event) {
        rabbitTemplate.convertAndSend(PARKING_LOT_KEY.concat(event.getKey()), event,
                message -> {
                    message.getMessageProperties()
                            .setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                }
        );
    }

    @Override
    public void saveBackupReservedInventoryEvent(String orderId) {
        rabbitTemplate.convertAndSend(RESERVED_ORDER_KEY, orderId,
                message -> {
                    message.getMessageProperties()
                            .setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                }
        );
    }
}

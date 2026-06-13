package com.cballestas.inventory_service.infrastructure.adapter.out.messaging.listener;

import com.cballestas.inventory_service.application.port.out.NotificationServiceMessagingPort;
import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class NotificationServiceListener {

    private final NotificationServiceMessagingPort notificationServiceMessagingPort;

    /**
     * Maneja el evento de inventario reservado, registrando el evento recibido y publicándolo
     * a través del puerto de mensajería de notificaciones.
     *
     * @param event evento de inventario reservado que contiene el identificador de la orden y la reserva
     */
    @EventListener
    public void handle(ReservedInventoryEvent event){
        log.info("Received reserved inventory event for orderId={}, reservationId={}", event.orderId(), event.reservationId());
        notificationServiceMessagingPort.publishReservedInventoryEvent(event);
    }

}

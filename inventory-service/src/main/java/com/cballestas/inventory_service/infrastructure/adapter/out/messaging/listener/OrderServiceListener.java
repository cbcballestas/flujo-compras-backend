package com.cballestas.inventory_service.infrastructure.adapter.out.messaging.listener;

import com.cballestas.inventory_service.application.port.out.OrderServiceMessagingPort;
import com.cballestas.inventory_service.domain.model.event.FailedInventoryEvent;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderServiceListener {

    private final OrderServiceMessagingPort orderServiceMessagingPort;

    /**
     * Maneja el evento de inventario fallido recibido desde el dominio.
     * Este método es invocado automáticamente cuando se publica un evento de tipo {@link FailedInventoryEvent} en el contexto de la aplicación.
     *
     * <p>Registra en el log la recepción del evento y delega la publicación del evento de inventario fallido al puerto de mensajería
     * para notificar al servicio de órdenes que la reserva de inventario ha fallado.</p>
     *
     * @param event el evento de inventario fallido que contiene el identificador de la orden y el mensaje de error
     */
    @EventListener
    public void handleFailedInventoryEvent(FailedInventoryEvent event) {
        log.info("Received failed inventory event for orderId={}, reason={}", event.orderId(), event.errorMessage());
        try {
            orderServiceMessagingPort.publishFailedReservedInventoryEvent(event);
        } catch (Exception e) {
            log.error("Error publishing failed inventory event for orderId={}", event.orderId(), e);
        }
    }

    @EventListener
    public void handleParkingLotEvent(OrderCreatedEvent event) {
        log.info("Received parking lot event for orderId={}, customerId={}", event.orderId(), event.customerId());
        orderServiceMessagingPort.publishParkingLotEvent(event);
    }

}

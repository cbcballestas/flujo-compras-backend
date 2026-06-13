package com.cballestas.order_service.infrastructure.adapter.out.messaging.listener;

import com.cballestas.order_service.application.port.out.InventoryServiceMessagingPort;
import com.cballestas.order_service.domain.model.dto.response.OrderResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderListener {

    private final InventoryServiceMessagingPort inventoryServiceMessagingPort;

    /**
     * Maneja los eventos de órdenes recibidos en el sistema. Este método es invocado automáticamente
     * cuando se publica un evento de tipo {@link OrderResponse} en el contexto de la aplicación.
     * <p>
     * Funcionalidad:
     * <ul>
     *     <li>Registra en el log la recepción del evento de orden, mostrando el ID y el estado de la orden.</li>
     *     <li>Publica el evento de creación de orden al servicio de inventario a través del puerto de mensajería.</li>
     * </ul>
     *
     * @param orderResponse el evento de orden recibido, que contiene información relevante de la orden.
     */
    @EventListener
    public void handler(OrderResponse orderResponse) {
        log.info("Received order created event: orderId={}, status={}", orderResponse.id(), orderResponse.status());
        inventoryServiceMessagingPort.publishOrderCreatedEvent(orderResponse);
    }

}

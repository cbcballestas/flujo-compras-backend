package com.cballestas.inventory_service.infrastructure.adapter.in.messaging.listener;

import com.cballestas.inventory_service.domain.model.event.OutBoxEvent;
import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.inventory_service.infrastructure.adapter.in.messaging.service.InventoryListenerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class InventoryListener {

    private final InventoryListenerService inventoryListenerService;

    /**
     * Maneja eventos de OutBox que requieren ser almacenados en la cola de estacionamiento.
     * <p>
     * Este método es invocado automáticamente cuando se publica un evento {@link OutBoxEvent}
     * en el contexto de aplicación de Spring. Se utiliza para procesar eventos que no pudieron
     * ser entregados exitosamente y necesitan ser reintegrados en el sistema de mensajería.
     * </p>
     *
     * @param event el evento {@link OutBoxEvent} que será procesado y almacenado en la cola de estacionamiento
     */
    @EventListener
    public void handleParkingLotEvent(OutBoxEvent event) {
        inventoryListenerService.saveBackupOutboxEvent(event);
    }

    /**
     * Maneja eventos de inventario reservado y los comunica a través del sistema de mensajería.
     * <p>
     * Este método es invocado automáticamente cuando se publica un evento de reserva de inventario
     * en el contexto de aplicación de Spring. Procesa la identificación de la orden y la envía
     * a la cola de procesamiento correspondiente para notificar a otros servicios que el inventario
     * ha sido reservado exitosamente.
     * </p>
     *
     * @param orderId identificador único de la orden cuyo inventario ha sido reservado
     */
    @EventListener
    public void handleReservedInventoryEvent(String orderId) {
        inventoryListenerService.saveBackupReservedInventoryEvent(orderId);
    }
}

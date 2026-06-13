package com.cballestas.inventory_service.application.port.out;

import com.cballestas.inventory_service.domain.model.InventoryReservation;

public interface InventoryReservationPersistencePort {
    InventoryReservation save(InventoryReservation inventoryReservation);
}

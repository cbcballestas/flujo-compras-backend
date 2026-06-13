package com.cballestas.inventory_service.application.port.out;

import com.cballestas.inventory_service.domain.model.Inventory;

import java.util.List;
import java.util.Optional;

public interface InventoryPersistencePort {
    List<Inventory> findAll();

    Optional<Inventory> findByProductId(String productId);

    Inventory save(Inventory inventory);
}

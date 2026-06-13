package com.cballestas.inventory_service.infrastructure.adapter.out.persistence.repository;

import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.entity.InventoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface InventoryJpaRepository extends JpaRepository<InventoryEntity, String> {
    Optional<InventoryEntity> findByProductId(String productId);
}

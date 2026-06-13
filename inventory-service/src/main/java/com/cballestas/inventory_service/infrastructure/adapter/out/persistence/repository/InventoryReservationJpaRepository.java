package com.cballestas.inventory_service.infrastructure.adapter.out.persistence.repository;

import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.entity.InventoryReservationEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InventoryReservationJpaRepository extends JpaRepository<InventoryReservationEntity, String> {
}

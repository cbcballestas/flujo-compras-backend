package com.cballestas.inventory_service.infrastructure.adapter.out.persistence.repository;

import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.entity.OutBoxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventJpaRepository extends JpaRepository<OutBoxEventEntity, String> {
}

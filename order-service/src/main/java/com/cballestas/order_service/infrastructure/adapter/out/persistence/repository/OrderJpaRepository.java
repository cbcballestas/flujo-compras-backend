package com.cballestas.order_service.infrastructure.adapter.out.persistence.repository;

import com.cballestas.order_service.domain.model.enums.OrderStatus;
import com.cballestas.order_service.infrastructure.adapter.out.persistence.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface OrderJpaRepository extends JpaRepository<OrderEntity, String> {

    List<OrderEntity> findByCustomerId(String customerId);

    List<OrderEntity> findByStatus(OrderStatus status);

    @Query("SELECT o FROM OrderEntity o WHERE o.createdAt >= :startDate")
    List<OrderEntity> findOrdersCreatedAfter(LocalDateTime startDate);

    @Query("SELECT o FROM OrderEntity o WHERE o.customerId = :customerId AND o.status = :status")
    List<OrderEntity> findByCustomerIdAndStatus(String customerId, OrderStatus status);

    Optional<OrderEntity> findByIdAndCustomerId(String id, String customerId);

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = :status")
    Long countByStatus(OrderStatus status);
}

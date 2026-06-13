package com.cballestas.order_service.application.port.out;

import com.cballestas.order_service.domain.model.Order;

import java.util.List;
import java.util.Optional;

public interface OrderPersistencePort {
    List<Order> getAll();

    Optional<Order> findById(String id);

    Order save(Order request);
}

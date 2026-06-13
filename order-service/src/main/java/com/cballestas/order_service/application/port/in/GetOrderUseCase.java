package com.cballestas.order_service.application.port.in;

import com.cballestas.order_service.domain.model.dto.response.OrderResponse;

import java.util.List;

public interface GetOrderUseCase {
    List<OrderResponse> getAll();

    OrderResponse findById(String id);
}

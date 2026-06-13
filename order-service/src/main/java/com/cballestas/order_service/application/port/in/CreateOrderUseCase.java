package com.cballestas.order_service.application.port.in;

import com.cballestas.order_service.domain.model.dto.request.OrderRequest;
import com.cballestas.order_service.domain.model.dto.response.OrderResponse;

public interface CreateOrderUseCase {
    OrderResponse save(OrderRequest orderRequest);
}

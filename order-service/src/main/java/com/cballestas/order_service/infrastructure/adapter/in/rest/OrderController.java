package com.cballestas.order_service.infrastructure.adapter.in.rest;

import com.cballestas.order_service.application.port.in.CreateOrderUseCase;
import com.cballestas.order_service.application.port.in.GetOrderUseCase;
import com.cballestas.order_service.domain.model.dto.request.OrderRequest;
import com.cballestas.order_service.domain.model.dto.response.OrderResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final GetOrderUseCase getOrderUseCase;
    private final CreateOrderUseCase createOrderUseCase;

    @GetMapping
    public ResponseEntity<List<OrderResponse>> findAll() {
        log.info("Received request to get all orders");
        return ResponseEntity.ok(getOrderUseCase.getAll());
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> findById(@PathVariable String id) {
        log.info("Received request to get order with id: {}", id);
        return ResponseEntity.ok(getOrderUseCase.findById(id));
    }

    @PostMapping
    public ResponseEntity<OrderResponse> save(@Valid @RequestBody OrderRequest orderRequest) {
        log.info("Received request to create order: {}", orderRequest);
        return ResponseEntity.ok(createOrderUseCase.save(orderRequest));
    }

}

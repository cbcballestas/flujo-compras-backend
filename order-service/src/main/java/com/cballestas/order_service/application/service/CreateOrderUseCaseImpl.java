package com.cballestas.order_service.application.service;

import com.cballestas.order_service.application.mapper.OrderRequestMapper;
import com.cballestas.order_service.application.mapper.OrderResponseMapper;
import com.cballestas.order_service.application.port.in.CreateOrderUseCase;
import com.cballestas.order_service.application.port.out.OrderPersistencePort;
import com.cballestas.order_service.domain.model.dto.request.OrderRequest;
import com.cballestas.order_service.domain.model.dto.response.OrderResponse;
import com.cballestas.order_service.domain.model.enums.OrderStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CreateOrderUseCaseImpl implements CreateOrderUseCase {

    private final OrderPersistencePort orderPersistencePort;
    private final ApplicationEventPublisher applicationEventPublisher;

    private final OrderRequestMapper orderRequestMapper;
    private final OrderResponseMapper orderResponseMapper;

    /**
     * Crea y almacena una nueva orden en el sistema.
     * <p>
     * Este método recibe un objeto de solicitud de orden (DTO), lo transforma en el modelo de dominio correspondiente
     * utilizando el mapeador, y luego lo persiste a través del puerto de persistencia. Finalmente, convierte la entidad
     * creada en un objeto de respuesta (DTO) para ser retornado al cliente o capa superior.
     *
     * @param orderRequest el objeto {@link OrderRequest} que contiene los datos de la orden a crear.
     * @return el objeto {@link OrderResponse} que representa la orden creada y almacenada.
     */
    @Transactional
    @Override
    public OrderResponse save(OrderRequest orderRequest) {
        var orderToCreate = orderRequestMapper.toDomain(orderRequest);

        // Calculate total amount
        Double totalAmount = orderRequest.items().stream()
                .mapToDouble(item -> item.price() * item.quantity())
                .sum();
        orderToCreate.setTotalAmount(totalAmount);

        orderToCreate.setStatus(OrderStatus.CREATED);

        var createdOrder = orderPersistencePort.save(orderToCreate);
        var orderResponse = orderResponseMapper.toResponse(createdOrder);

        // Publicar el evento de orden creada en el contexto de la aplicación para que los listeners puedan manejarlo
        applicationEventPublisher.publishEvent(orderResponse);

        return orderResponse;
    }
}

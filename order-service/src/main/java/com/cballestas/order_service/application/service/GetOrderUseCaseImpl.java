package com.cballestas.order_service.application.service;

import com.cballestas.order_service.application.mapper.OrderResponseMapper;
import com.cballestas.order_service.application.port.in.GetOrderUseCase;
import com.cballestas.order_service.application.port.out.OrderPersistencePort;
import com.cballestas.order_service.domain.model.dto.response.OrderResponse;
import com.cballestas.order_service.infrastructure.adapter.in.rest.exception.OrderNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class GetOrderUseCaseImpl implements GetOrderUseCase {

    private final OrderPersistencePort orderPersistencePort;
    private final OrderResponseMapper orderResponseMapper;


    /**
     * Obtiene todas las órdenes existentes en el sistema.
     * <p>
     * Este método recupera todas las entidades de órdenes almacenadas a través del puerto de persistencia,
     * las transforma en objetos de respuesta (DTO) utilizando el mapeador correspondiente y retorna la lista resultante.
     * Es útil para mostrar o procesar todas las órdenes registradas.
     *
     * @return una lista de objetos {@link OrderResponse} que representan todas las órdenes existentes.
     */
    @Transactional(readOnly = true)
    @Override
    public List<OrderResponse> getAll() {
        return orderPersistencePort.getAll().stream()
                .map(orderResponseMapper::toResponse)
                .toList();
    }

    /**
     * Busca una orden específica por su identificador único.
     * <p>
     * Este método intenta recuperar una orden utilizando su UUID a través del puerto de persistencia.
     * Si la orden existe, la transforma en un objeto de respuesta (DTO) usando el mapeador correspondiente.
     * Si no se encuentra la orden, lanza una excepción indicando que no existe una orden con el id proporcionado.
     *
     * @param id el identificador único de la orden a buscar.
     * @return el objeto {@link OrderResponse} que representa la orden encontrada.
     * @throws OrderNotFoundException si no se encuentra una orden con el id especificado.
     */
    @Transactional(readOnly = true)
    @Override
    public OrderResponse findById(String id) {
        return orderPersistencePort.findById(id)
                .map(orderResponseMapper::toResponse)
                .orElseThrow(() -> new OrderNotFoundException("Order not found with id: " + id));
    }
}

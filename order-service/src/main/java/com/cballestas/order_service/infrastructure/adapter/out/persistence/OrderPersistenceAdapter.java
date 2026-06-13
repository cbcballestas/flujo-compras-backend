package com.cballestas.order_service.infrastructure.adapter.out.persistence;

import com.cballestas.order_service.application.port.out.OrderPersistencePort;
import com.cballestas.order_service.domain.model.Order;
import com.cballestas.order_service.infrastructure.adapter.out.persistence.mapper.OrderPersistenceMapper;
import com.cballestas.order_service.infrastructure.adapter.out.persistence.repository.OrderJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class OrderPersistenceAdapter implements OrderPersistencePort {

    private final OrderJpaRepository repository;
    private final OrderPersistenceMapper mapper;

    /**
     * Recupera todas las órdenes almacenadas en la base de datos.
     * <p>
     * Este método consulta el repositorio JPA para obtener todas las entidades de órdenes persistidas en la base de datos.
     * Posteriormente, utiliza el mapeador de persistencia para transformar cada entidad a su correspondiente modelo de dominio {@link Order}.
     * El resultado es una lista de objetos de dominio que representan el estado actual de todas las órdenes almacenadas.
     * <p>
     * Este método es útil para operaciones de consulta masiva, reportes o visualización de todas las órdenes existentes.
     *
     * @return una lista de objetos {@link Order} que representan todas las órdenes almacenadas en la base de datos.
     */
    @Override
    public List<Order> getAll() {
        return repository.findAll().stream()
                .map(mapper::toDomain)
                .toList();
    }

    /**
     * Busca y recupera una orden específica a partir de su identificador único.
     * <p>
     * Este método utiliza el repositorio JPA para buscar una entidad de orden por su id primario. Si la entidad existe,
     * se transforma al modelo de dominio {@link Order} mediante el mapeador de persistencia. Si no se encuentra ninguna entidad
     * con el id proporcionado, se retorna un {@link Optional} vacío, permitiendo así un manejo seguro de la ausencia de resultados.
     * <p>
     * Este método es fundamental para operaciones de consulta puntual, validaciones o flujos donde se requiere acceder a una orden específica.
     *
     * @param id el identificador único de la orden a buscar en la base de datos.
     * @return un {@link Optional} que contiene la orden encontrada como objeto de dominio, o vacío si no existe ninguna orden con ese id.
     */
    @Override
    public Optional<Order> findById(String id) {
        return repository.findById(id)
                .map(mapper::toDomain);
    }

    /**
     * Crea y persiste una nueva orden en la base de datos a partir de un modelo de dominio.
     * <p>
     * Este método recibe un objeto {@link Order} del dominio, lo transforma a una entidad persistente utilizando el mapeador,
     * y agrega los ítems asociados a la orden. Luego, la entidad es almacenada en la base de datos mediante el repositorio JPA.
     * Finalmente, la entidad persistida se convierte nuevamente a un objeto de dominio para ser retornado, reflejando el estado
     * final de la orden tras la persistencia (incluyendo posibles valores generados por la base de datos, como el id).
     * <p>
     * Este método es esencial para flujos de creación de órdenes, asegurando la correcta transformación y almacenamiento de los datos.
     *
     * @param request el objeto {@link Order} que representa la orden a crear y persistir en la base de datos.
     * @return el objeto {@link Order} resultante tras la persistencia, reflejando el estado final de la orden creada.
     */
    @Override
    public Order save(Order request) {

        var orderToSave = mapper.toEntity(request);
        orderToSave.addItems(orderToSave.getItems());

        var savedOrder = repository.save(orderToSave);
        return mapper.toDomain(savedOrder);
    }
}

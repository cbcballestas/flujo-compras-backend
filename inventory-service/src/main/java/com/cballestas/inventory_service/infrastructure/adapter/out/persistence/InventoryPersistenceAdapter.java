package com.cballestas.inventory_service.infrastructure.adapter.out.persistence;

import com.cballestas.inventory_service.application.port.out.InventoryPersistencePort;
import com.cballestas.inventory_service.domain.model.Inventory;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.mapper.InventoryPersistenceMapper;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.repository.InventoryJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class InventoryPersistenceAdapter implements InventoryPersistencePort {

    private final InventoryJpaRepository repository;
    private final InventoryPersistenceMapper mapper;

    /**
     * Recupera todos los registros de inventario almacenados en la base de datos.
     * Utiliza el repositorio JPA para obtener todas las entidades y las mapea al modelo de dominio.
     *
     * @return una lista de objetos {@link Inventory} que representan todos los inventarios existentes
     */
    @Override
    public List<Inventory> findAll() {
        return mapper.toDomainList(repository.findAll());
    }

    /**
     * Busca un inventario específico por el identificador del producto.
     *
     * @param productId identificador único del producto a buscar en el inventario
     * @return un {@link Optional} que contiene el inventario encontrado o vacío si no existe
     */
    @Override
    public Optional<Inventory> findByProductId(String productId) {
        return repository.findByProductId(productId)
                .map(mapper::toDomain);
    }

    /**
     * Guarda o actualiza un registro de inventario en la base de datos.
     * Convierte el modelo de dominio a entidad, lo persiste y retorna el modelo actualizado.
     *
     * @param inventory objeto {@link Inventory} a guardar o actualizar
     * @return el objeto {@link Inventory} persistido con los datos actualizados
     */
    @Override
    public Inventory save(Inventory inventory) {
        var inventoryEntity = mapper.toEntity(inventory);
        return mapper.toDomain(repository.save(inventoryEntity));
    }
}

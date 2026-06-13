package com.cballestas.inventory_service.infrastructure.adapter.out.persistence;

import com.cballestas.inventory_service.application.port.out.InventoryReservationPersistencePort;
import com.cballestas.inventory_service.domain.model.InventoryReservation;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.mapper.InventoryReservationPersistenceMapper;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.repository.InventoryReservationJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class InventoryReservationPersistenceAdapter implements InventoryReservationPersistencePort {

    private final InventoryReservationJpaRepository repository;
    private final InventoryReservationPersistenceMapper mapper;

    /**
     * Guarda o actualiza una reserva de inventario en la base de datos.
     * Convierte el modelo de dominio a entidad, lo persiste y retorna el modelo actualizado.
     *
     * @param inventoryReservation objeto {@link InventoryReservation} a guardar o actualizar
     * @return el objeto {@link InventoryReservation} persistido con los datos actualizados
     */
    @Override
    public InventoryReservation save(InventoryReservation inventoryReservation) {
        var inventoryReservationEntity = mapper.toEntity(inventoryReservation);
        return mapper.toDomain(repository.save(inventoryReservationEntity));
    }
}

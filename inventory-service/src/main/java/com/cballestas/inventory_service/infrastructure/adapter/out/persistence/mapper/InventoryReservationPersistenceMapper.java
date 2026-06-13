package com.cballestas.inventory_service.infrastructure.adapter.out.persistence.mapper;

import com.cballestas.inventory_service.domain.model.InventoryReservation;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.entity.InventoryReservationEntity;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface InventoryReservationPersistenceMapper {

    InventoryReservation toDomain(InventoryReservationEntity entity);

    InventoryReservationEntity toEntity(InventoryReservation domain);
}

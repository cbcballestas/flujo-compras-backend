package com.cballestas.inventory_service.infrastructure.adapter.out.persistence.mapper;

import com.cballestas.inventory_service.domain.model.Inventory;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.entity.InventoryEntity;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

import java.util.List;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface InventoryPersistenceMapper {

    Inventory toDomain(InventoryEntity inventoryEntity);

    InventoryEntity toEntity(Inventory inventory);

    List<Inventory> toDomainList(List<InventoryEntity> inventoryEntities);
}

package com.cballestas.inventory_service.infrastructure.adapter.out.persistence.mapper;

import com.cballestas.inventory_service.domain.model.event.OutBoxEvent;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.entity.OutBoxEventEntity;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OutboxEventPersistenceMapper {

    OutBoxEvent toDomain(OutBoxEventEntity entity);

    OutBoxEventEntity toEntity(OutBoxEvent event);
}

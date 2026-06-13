package com.cballestas.notification_service.infrastructure.out.persistence.mapper;

import com.cballestas.notification_service.domain.model.OutBoxEvent;
import com.cballestas.notification_service.infrastructure.out.persistence.entity.NotificationOutboxDocument;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OutboxEventPersistenceMapper {

    OutBoxEvent toDomain(NotificationOutboxDocument document);

    NotificationOutboxDocument toDocument(OutBoxEvent event);
}

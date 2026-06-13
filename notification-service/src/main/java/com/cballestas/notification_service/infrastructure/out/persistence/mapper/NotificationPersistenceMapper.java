package com.cballestas.notification_service.infrastructure.out.persistence.mapper;

import com.cballestas.notification_service.domain.model.Notification;
import com.cballestas.notification_service.infrastructure.out.persistence.entity.NotificationDocument;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface NotificationPersistenceMapper {

    NotificationDocument toEntity(Notification notification);

    Notification toDomain(NotificationDocument entity);
}

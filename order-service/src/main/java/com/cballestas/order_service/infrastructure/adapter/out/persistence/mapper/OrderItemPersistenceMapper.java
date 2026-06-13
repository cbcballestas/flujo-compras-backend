package com.cballestas.order_service.infrastructure.adapter.out.persistence.mapper;

import com.cballestas.order_service.domain.model.OrderItem;
import com.cballestas.order_service.infrastructure.adapter.out.persistence.entity.OrderItemEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OrderItemPersistenceMapper {
    OrderItemEntity toEntity(OrderItem orderItem);

    @Mapping(target = "order", ignore = true)
    OrderItem toDomain(OrderItemEntity orderItemEntity);
}

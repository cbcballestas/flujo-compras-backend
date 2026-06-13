package com.cballestas.order_service.infrastructure.adapter.out.persistence.mapper;

import com.cballestas.order_service.domain.model.Order;
import com.cballestas.order_service.infrastructure.adapter.out.persistence.entity.OrderEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, uses = {OrderItemPersistenceMapper.class}, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OrderPersistenceMapper {

    @Mapping(target = "items", source = "orderItems")
    OrderEntity toEntity(Order order);

    @Mapping(target = "orderItems", source = "items")
    Order toDomain(OrderEntity orderEntity);
}

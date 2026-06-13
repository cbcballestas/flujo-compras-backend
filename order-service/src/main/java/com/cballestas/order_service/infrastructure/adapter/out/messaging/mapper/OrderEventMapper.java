package com.cballestas.order_service.infrastructure.adapter.out.messaging.mapper;

import com.cballestas.order_service.domain.model.Order;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;
import com.cballestas.order_service.domain.model.dto.response.OrderResponse;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, uses = {OrderItemEventMapper.class}, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OrderEventMapper {

    @Mapping(target = "orderId", source = "id")
    OrderCreatedEvent toEvent(OrderResponse order);

    @Mapping(target = "id", source = "orderId")
    Order toDomain(OrderCreatedEvent event);
}

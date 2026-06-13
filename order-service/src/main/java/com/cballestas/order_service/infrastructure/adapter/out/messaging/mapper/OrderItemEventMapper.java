package com.cballestas.order_service.infrastructure.adapter.out.messaging.mapper;

import com.cballestas.order_service.domain.model.OrderItem;
import com.cballestas.order_service.domain.model.dto.event.OrderItemEvent;
import com.cballestas.order_service.domain.model.dto.response.OrderItemResponse;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OrderItemEventMapper {

    OrderItemEvent toEvent(OrderItemResponse orderItem);

    OrderItem toDomain(OrderItemEvent orderItem);
}

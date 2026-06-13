package com.cballestas.order_service.application.mapper;

import com.cballestas.order_service.domain.model.Order;
import com.cballestas.order_service.domain.model.dto.response.OrderResponse;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, uses = {OrderItemResponseMapper.class}, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OrderResponseMapper {

    @Mapping(source = "orderItems", target = "items")
    OrderResponse toResponse(Order order);
}

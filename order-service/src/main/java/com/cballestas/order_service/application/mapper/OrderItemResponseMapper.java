package com.cballestas.order_service.application.mapper;

import com.cballestas.order_service.domain.model.OrderItem;
import com.cballestas.order_service.domain.model.dto.response.OrderItemResponse;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OrderItemResponseMapper {

    OrderItemResponse toResponse(OrderItem orderItem);
}

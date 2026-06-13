package com.cballestas.order_service.application.mapper;

import com.cballestas.order_service.domain.model.Order;
import com.cballestas.order_service.domain.model.dto.request.OrderRequest;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, uses = {OrderItemRequestMapper.class}, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface OrderRequestMapper {

    @Mapping(source = "items", target = "orderItems")
    Order toDomain(OrderRequest orderRequest);
}

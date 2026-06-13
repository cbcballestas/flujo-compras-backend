package com.cballestas.order_service.domain.model.enums;

public enum OrderStatus {
    CREATED,
    PENDING_INVENTORY,
    INVENTORY_RESERVED,
    PENDING_PAYMENT,
    PAYMENT_PROCESSED,
    CONFIRMED,
    COMPLETED,
    CANCELLED,
    FAILED,
    BACKUP
}

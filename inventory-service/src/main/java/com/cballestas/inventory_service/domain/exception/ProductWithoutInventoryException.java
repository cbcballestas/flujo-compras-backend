package com.cballestas.inventory_service.domain.exception;

public class ProductWithoutInventoryException extends RuntimeException {
    public ProductWithoutInventoryException(String message) {
        super(message);
    }
}

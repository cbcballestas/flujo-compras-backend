package com.cballestas.inventory_service.domain.model;

import com.cballestas.inventory_service.domain.exception.InsufficientStockException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Inventory {
    private String id;
    private String productId;
    private String productName;
    private Integer quantity;
    private Integer reserved;
    private Integer available;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public boolean canReserve(Integer amount) {
        return available >= amount;
    }

    public void reserve(Integer amount) {
        if (!canReserve(amount)) {
            throw new InsufficientStockException("Insufficient inventory for product: " + productId);
        }
        this.reserved += amount;
        this.available -= amount;
    }

    public void release(Integer amount) {
        this.reserved -= amount;
        this.available += amount;
    }

    public void addStock(Integer amount) {
        this.quantity += amount;
        this.available += amount;
    }
}

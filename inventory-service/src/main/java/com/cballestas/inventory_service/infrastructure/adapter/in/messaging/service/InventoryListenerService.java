package com.cballestas.inventory_service.infrastructure.adapter.in.messaging.service;

import com.cballestas.inventory_service.domain.model.event.OutBoxEvent;

public interface InventoryListenerService {
    void handleBackupOutboxEvent(OutBoxEvent event);
    void saveBackupReservedInventoryEvent(String orderId);
}

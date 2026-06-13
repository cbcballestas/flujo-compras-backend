package com.cballestas.notification_service.application.ports.in;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;

public interface NotificationServicePort {
    void send(ReservedInventoryEvent event);
}

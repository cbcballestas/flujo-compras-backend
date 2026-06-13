package com.cballestas.inventory_service.application.port.out;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;

public interface NotificationServiceMessagingPort {
    void publishReservedInventoryEvent(ReservedInventoryEvent event);
}

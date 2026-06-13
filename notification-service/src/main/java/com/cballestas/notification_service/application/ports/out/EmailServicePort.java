package com.cballestas.notification_service.application.ports.out;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;

public interface EmailServicePort {
    void sendEmail(ReservedInventoryEvent event);
}

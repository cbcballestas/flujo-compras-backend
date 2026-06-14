package com.cballestas.notification_service.infrastructure.out.messaging.dto;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;

public record FailedNotificationEvent(
        ReservedInventoryEvent reservedInventoryEvent,
        String errorMessage
) {
}

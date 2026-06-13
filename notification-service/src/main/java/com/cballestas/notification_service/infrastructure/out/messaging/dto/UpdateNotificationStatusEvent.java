package com.cballestas.notification_service.infrastructure.out.messaging.dto;

import com.cballestas.notification_service.domain.enums.NotificationStatus;
import lombok.Builder;

@Builder
public record UpdateNotificationStatusEvent(
        String orderId,
        String reservationId,
        String customerId,
        NotificationStatus status
) {
}

package com.cballestas.notification_service.domain.model;

import com.cballestas.notification_service.domain.enums.NotificationStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Notification {
    private String id;
    private String orderId;
    private String reservationId;
    private String customerId;
    private Double totalAmount;
    private NotificationStatus status;
    private String message;
}

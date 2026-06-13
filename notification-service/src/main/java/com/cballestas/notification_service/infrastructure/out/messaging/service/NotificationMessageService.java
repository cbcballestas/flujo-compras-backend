package com.cballestas.notification_service.infrastructure.out.messaging.service;

import com.cballestas.notification_service.infrastructure.out.messaging.dto.FailedNotificationEvent;
import com.cballestas.notification_service.infrastructure.out.messaging.dto.UpdateNotificationStatusEvent;

public interface NotificationMessageService {
    void saveParkingLot(FailedNotificationEvent failedNotificationEvent);
    void updateNotificationStatus(UpdateNotificationStatusEvent updateNotificationStatusEvent);
}

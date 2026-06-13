package com.cballestas.notification_service.application.ports.out;

import com.cballestas.notification_service.domain.model.Notification;

import java.util.List;
import java.util.Optional;

public interface NotificationPersistencePort {
    List<Notification> findAll();

    Optional<Notification> findByOrderIdAndReservationIdAndCustomerId(String orderId, String reservationId, String customerId);

    Optional<Notification> findById(String id);

    Notification save(Notification notification);
}

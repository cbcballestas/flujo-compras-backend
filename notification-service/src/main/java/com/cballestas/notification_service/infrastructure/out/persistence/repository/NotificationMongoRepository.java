package com.cballestas.notification_service.infrastructure.out.persistence.repository;

import com.cballestas.notification_service.infrastructure.out.persistence.entity.NotificationDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface NotificationMongoRepository extends MongoRepository<NotificationDocument, String> {

    Optional<NotificationDocument> findById(String id);

    Optional<NotificationDocument> findByOrderIdAndReservationIdAndCustomerId(String orderId, String reservationId, String customerId);
}

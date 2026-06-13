package com.cballestas.notification_service.infrastructure.out.persistence.repository;

import com.cballestas.notification_service.infrastructure.out.persistence.entity.NotificationOutboxDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface NotificationOutBoxMongoRepository extends MongoRepository<NotificationOutboxDocument, String> {

}

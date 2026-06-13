package com.cballestas.notification_service.infrastructure.out.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "notifications_outbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationOutboxDocument {

    @Id
    private String id;
    private String key;
    private String payload;
    private String topic;
    private Boolean published;
    private LocalDateTime createdAt;
}

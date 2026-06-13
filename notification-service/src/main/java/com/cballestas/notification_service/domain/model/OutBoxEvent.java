package com.cballestas.notification_service.domain.model;

import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class OutBoxEvent {
    private String id;
    private String key;
    private String payload;
    private String topic;
    private Boolean published;
    private LocalDateTime createdAt;
}

package com.cballestas.inventory_service.domain.model.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

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
    private String errorMessage;
    private Boolean published;
    private LocalDateTime createdAt;
}

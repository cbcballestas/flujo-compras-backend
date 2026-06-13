package com.cballestas.notification_service.application.ports.out;

import com.cballestas.notification_service.domain.model.OutBoxEvent;

public interface OutboxEventPersistencePort {
    OutBoxEvent save(OutBoxEvent event);
}

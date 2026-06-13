package com.cballestas.inventory_service.application.port.out;

import com.cballestas.inventory_service.domain.model.event.OutBoxEvent;

public interface OutboxEventPersistencePort {
    OutBoxEvent save(OutBoxEvent event);
}

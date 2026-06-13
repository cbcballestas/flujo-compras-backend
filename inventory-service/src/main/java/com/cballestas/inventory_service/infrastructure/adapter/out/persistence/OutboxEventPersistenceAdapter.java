package com.cballestas.inventory_service.infrastructure.adapter.out.persistence;

import com.cballestas.inventory_service.application.port.out.OutboxEventPersistencePort;
import com.cballestas.inventory_service.domain.model.event.OutBoxEvent;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.mapper.OutboxEventPersistenceMapper;
import com.cballestas.inventory_service.infrastructure.adapter.out.persistence.repository.OutboxEventJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Adaptador de persistencia para eventos de outbox en el contexto de inventario.
 * Implementa el puerto de salida {@link OutboxEventPersistencePort} y se encarga de
 * almacenar y recuperar eventos de outbox utilizando un repositorio JPA y un mapper
 * para la conversión entre entidades de persistencia y modelos de dominio.
 */
@Component
@RequiredArgsConstructor
public class OutboxEventPersistenceAdapter implements OutboxEventPersistencePort {

    private final OutboxEventJpaRepository repository;
    private final OutboxEventPersistenceMapper mapper;

    /**
     * Persiste un evento de outbox en la base de datos y retorna el evento guardado
     * como modelo de dominio.
     *
     * @param event instancia de {@link OutBoxEvent} que representa el evento a persistir
     * @return el evento de outbox guardado, convertido nuevamente a modelo de dominio
     */
    @Override
    public OutBoxEvent save(OutBoxEvent event) {
        var outboxEventEntity = mapper.toEntity(event);
        var savedOutboxEvent = repository.save(outboxEventEntity);
        return mapper.toDomain(savedOutboxEvent);
    }
}

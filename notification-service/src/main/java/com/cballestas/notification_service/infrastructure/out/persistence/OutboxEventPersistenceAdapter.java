package com.cballestas.notification_service.infrastructure.out.persistence;

import com.cballestas.notification_service.application.ports.out.OutboxEventPersistencePort;
import com.cballestas.notification_service.domain.model.OutBoxEvent;
import com.cballestas.notification_service.infrastructure.out.persistence.mapper.OutboxEventPersistenceMapper;
import com.cballestas.notification_service.infrastructure.out.persistence.repository.NotificationOutBoxMongoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Adaptador de persistencia para eventos de outbox en el contexto de notificaciones.
 * Implementa el puerto de salida {@link OutboxEventPersistencePort} y se encarga de
 * almacenar eventos de outbox en la base de datos MongoDB utilizando el repositorio
 * y el mapper correspondientes.
 */
@Component
@RequiredArgsConstructor
public class OutboxEventPersistenceAdapter implements OutboxEventPersistencePort {

    /**
     * Repositorio Mongo para la persistencia de eventos de outbox de notificación.
     */
    private final NotificationOutBoxMongoRepository repository;
    /**
     * Mapper para convertir entre el modelo de dominio y el documento de persistencia.
     */
    private final OutboxEventPersistenceMapper mapper;

    /**
     * Persiste un evento de outbox en la base de datos MongoDB y retorna el evento guardado
     * como modelo de dominio. Utiliza el mapper para convertir el evento de dominio a documento,
     * lo almacena mediante el repositorio y luego lo convierte de nuevo a modelo de dominio.
     *
     * @param event instancia de {@link OutBoxEvent} que representa el evento a persistir
     * @return el evento de outbox guardado, convertido nuevamente a modelo de dominio
     * @throws IllegalArgumentException si el evento es nulo o no puede ser mapeado correctamente
     */
    @Override
    public OutBoxEvent save(OutBoxEvent event) {
        var outboxEventDocument = mapper.toDocument(event);
        var savedOutboxEvent = repository.save(outboxEventDocument);
        return mapper.toDomain(savedOutboxEvent);
    }
}

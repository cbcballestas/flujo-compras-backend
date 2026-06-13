package com.cballestas.notification_service.infrastructure.out.persistence;

import com.cballestas.notification_service.application.ports.out.NotificationPersistencePort;
import com.cballestas.notification_service.domain.model.Notification;
import com.cballestas.notification_service.infrastructure.out.persistence.mapper.NotificationPersistenceMapper;
import com.cballestas.notification_service.infrastructure.out.persistence.repository.NotificationMongoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * Adaptador de persistencia para notificaciones.
 * Implementa el puerto de salida {@link NotificationPersistencePort} y gestiona
 * la interacción con la base de datos MongoDB a través del repositorio y el mapper.
 */
@Component
@RequiredArgsConstructor
public class NotificationPersistenceAdapter implements NotificationPersistencePort {

    private final NotificationMongoRepository repository;
    private final NotificationPersistenceMapper mapper;

    /**
     * Guarda una notificación en la base de datos MongoDB.
     *
     * @param notification objeto {@link Notification} a persistir
     * @return la notificación guardada, convertida al modelo de dominio
     */
    @Override
    public Notification save(Notification notification) {
        var entity = mapper.toEntity(notification);
        var saved = repository.save(entity);
        return mapper.toDomain(saved);
    }

    /**
     * Recupera todas las notificaciones almacenadas en la base de datos.
     *
     * @return lista de notificaciones en formato de dominio
     */
    @Override
    public List<Notification> findAll() {
        return repository.findAll().stream()
                .map(mapper::toDomain)
                .toList();
    }

    /**
     * Busca una notificación por orderId, reservationId y customerId.
     *
     * @param orderId identificador de la orden
     * @param reservationId identificador de la reserva
     * @param customerId identificador del cliente
     * @return un {@link Optional} con la notificación encontrada, o vacío si no existe
     */
    @Override
    public Optional<Notification> findByOrderIdAndReservationIdAndCustomerId(String orderId, String reservationId, String customerId) {
        return repository.findByOrderIdAndReservationIdAndCustomerId(orderId, reservationId, customerId)
                .map(mapper::toDomain);
    }

    /**
     * Busca una notificación por su identificador único.
     *
     * @param id identificador único de la notificación
     * @return un {@link Optional} con la notificación encontrada, o vacío si no existe
     */
    @Override
    public Optional<Notification> findById(String id) {
        return repository.findById(id)
                .map(mapper::toDomain);
    }
}

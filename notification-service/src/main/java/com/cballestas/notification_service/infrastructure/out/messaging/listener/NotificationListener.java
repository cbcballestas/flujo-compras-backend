package com.cballestas.notification_service.infrastructure.out.messaging.listener;

import com.cballestas.notification_service.infrastructure.out.messaging.dto.FailedNotificationEvent;
import com.cballestas.notification_service.infrastructure.out.messaging.dto.UpdateNotificationStatusEvent;
import com.cballestas.notification_service.infrastructure.out.messaging.service.NotificationMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class NotificationListener {

    private final NotificationMessageService notificationMessageService;

    /**
     * Maneja eventos de notificaciones fallidas almacenando el evento en la cola de espera (parking lot).
     * Este método se ejecuta dentro de una transacción para garantizar la consistencia de los datos
     * cuando se persisten eventos que no pudieron procesarse correctamente.
     *
     * @param failedNotificationEvent evento que contiene los detalles de la notificación fallida a almacenar
     */
    @EventListener
    public void handleParkingLotEvent(FailedNotificationEvent failedNotificationEvent) {
        notificationMessageService.saveParkingLot(failedNotificationEvent);
    }

    /**
     * Maneja eventos de actualización del estado de notificaciones.
     * Este método es responsable de procesar cambios en el estado de las notificaciones enviadas
     * y realizar las acciones correspondientes según el nuevo estado.
     *
     * @param updateNotificationStatusEvent evento que contiene los detalles de actualización del estado de la notificación
     */
    @EventListener
    public void handleUpdateNotificationStatusEvent(UpdateNotificationStatusEvent updateNotificationStatusEvent) {
        notificationMessageService.updateNotificationStatus(updateNotificationStatusEvent);
    }
}

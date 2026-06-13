package com.cballestas.notification_service.infrastructure.out.email;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.notification_service.application.ports.out.EmailServicePort;
import jakarta.mail.MessagingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

/**
 * Adaptador para el envío de correos electrónicos de confirmación de reserva de órdenes.
 * Implementa el puerto de salida {@link EmailServicePort} y utiliza JavaMailSender y Thymeleaf
 * para construir y enviar correos electrónicos personalizados a los clientes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EmailServiceAdapter implements EmailServicePort {

    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine;

    @Value("${spring.mail.username}")
    private String fromEmail;

    @Value("${app.mail.subject:Order Reservation Confirmation}")
    private String subject;

    /**
     * Envía un correo electrónico de confirmación de reserva de orden al cliente.
     * Utiliza una plantilla HTML y los datos del evento para personalizar el mensaje.
     *
     * @param event evento {@link ReservedInventoryEvent} con los datos de la orden y destinatario
     * @throws RuntimeException si ocurre un error al construir o enviar el correo electrónico
     */
    @Override
    public void sendEmail(ReservedInventoryEvent event) {
        try {
            var context = new Context();
            context.setVariable("orderId", event.orderId());
            context.setVariable("reservationId", event.reservationId());
            context.setVariable("customerId", event.customerId());
            context.setVariable("totalAmount", event.totalAmount());
            context.setVariable("items", event.items());
            context.setVariable("status", event.status());

            String htmlContent = templateEngine.process("order-confirmation", context);

            var message = mailSender.createMimeMessage();
            var helper = new MimeMessageHelper(message, true, "UTF-8");
            helper.setFrom(fromEmail);
            helper.setTo(fromEmail);
            helper.setSubject(subject);
            helper.setText(htmlContent, true);

            mailSender.send(message);
            log.info("Email sent successfully for orderId={}, reservationId={}", event.orderId(), event.reservationId());

        } catch (MessagingException e) {
            log.error("Failed to send email for orderId={}: {}", event.orderId(), e.getMessage(), e);
            throw new RuntimeException("Error sending email for order " + event.orderId(), e);
        }
    }
}

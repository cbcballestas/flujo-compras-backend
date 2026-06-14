package com.cballestas.notification_service.infrastructure.config;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Inventory Topics
    @Value("${app.kafka.topics.notification-confirmed}")
    private String confirmedOrdersTopic;

    @Value("${app.kafka.topics.log-error}")
    private String notificationErrorsTopic;

    /**
     * Crea y configura un ProducerFactory para Kafka, permitiendo la generación de productores
     * que envían mensajes serializados como String al clúster de Kafka.
     *
     * @return ProducerFactory configurado para productores String-String.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public ProducerFactory<String, ReservedInventoryEvent> reservedInventoryProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonSerializer.class);

        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean(name = "reservedInventoryKafkaTemplate")
    public KafkaTemplate<String, ReservedInventoryEvent> reservedInventoryKafkaTemplate() {
        return new KafkaTemplate<>(reservedInventoryProducerFactory());
    }

    /**
     * Crea y configura un ConsumerFactory para Kafka, permitiendo la generación de consumidores
     * que reciben y deserializan mensajes de tipo ReservedInventoryEvent desde el clúster de Kafka.
     *
     * @return ConsumerFactory configurado para ReservedInventoryEvent.
     */
    @Bean
    public ConsumerFactory<String, ReservedInventoryEvent> reservedIventoryConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent");
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Proporciona un ConcurrentKafkaListenerContainerFactory para gestionar la recepción concurrente
     * de mensajes de Kafka del tipo ReservedInventoryEvent.
     *
     * @return ConcurrentKafkaListenerContainerFactory para ReservedInventoryEvent.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ReservedInventoryEvent> reservedInventoryListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ReservedInventoryEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(reservedIventoryConsumerFactory());
        return factory;
    }

    // ========== ORDER TOPICS ==========

    /**
     * Crea el tópico de Kafka para eventos de órdenes confirmadas.
     *
     * @return NewTopic para órdenes confirmadas.
     */
    @Bean
    public NewTopic confirmedOrderTopic() {
        return TopicBuilder.name(confirmedOrdersTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * Crea el tópico de Kafka para eventos de errores de notificación.
     *
     * @return NewTopic para errores de notificación.
     */
    @Bean
    public NewTopic notificationLogErrorTopic() {
        return TopicBuilder.name(notificationErrorsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

}

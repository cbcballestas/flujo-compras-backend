package com.cballestas.inventory_service.infrastructure.adapter.config;

import com.cballestas.inventory_service.domain.model.event.ReservedInventoryEvent;
import com.cballestas.order_service.domain.model.dto.event.OrderCreatedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
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
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Inventory Topics
    @Value("${app.kafka.topics.inventory-reserved}")
    private String inventoryReservedTopic;

    @Value("${app.kafka.topics.inventory-failed}")
    private String inventoryFailedTopic;

    @Value("${app.kafka.topics.log-error}")
    private String inventoryErrorsTopic;

    /**
     * Crea y configura un ProducerFactory para Kafka, permitiendo la generación de productores
     * que envían mensajes serializados como String al clúster de Kafka.
     * <p>
     * Funcionalidad:
     * <ul>
     *     <li>Define los parámetros de conexión al clúster Kafka.</li>
     *     <li>Configura la serialización de clave y valor como String.</li>
     *     <li>Activa la idempotencia y la confirmación de mensajes para garantizar la entrega única.</li>
     *     <li>Permite la reintentos automáticos y controla la cantidad de solicitudes en vuelo.</li>
     * </ul>
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
    public ProducerFactory<String, OrderCreatedEvent> orderCreatedEventProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Crea y configura un KafkaProducer especializado para enviar eventos de inventario reservado
     * serializados como JSON al clúster de Kafka.
     * <p>
     * Funcionalidad:
     * <ul>
     *     <li>Serializa la clave como String y el valor como JSON (ReservedInventoryEvent).</li>
     *     <li>Activa la idempotencia y la confirmación de mensajes para evitar duplicados.</li>
     *     <li>Permite reintentos automáticos y controla la concurrencia de solicitudes.</li>
     *     <li>Devuelve una instancia lista para enviar eventos de inventario reservado.</li>
     * </ul>
     *
     * @return KafkaProducer configurado para eventos ReservedInventoryEvent.
     */
    @Bean
    public KafkaProducer<String, ReservedInventoryEvent> kafkaProducer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        config.put(ProducerConfig.RETRIES_CONFIG, 3);//Integer.toString(Integer.MAX_VALUE)
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return new KafkaProducer<>(config);
    }

    /**
     * Proporciona un KafkaTemplate para enviar mensajes String al clúster de Kafka de forma sencilla.
     * <p>
     * Funcionalidad:
     * <ul>
     *     <li>Facilita el envío de mensajes a los tópicos de Kafka.</li>
     *     <li>Utiliza el ProducerFactory previamente configurado.</li>
     * </ul>
     *
     * @return KafkaTemplate para mensajes String.
     */
    @Bean
    public KafkaTemplate<String, String> defaultRetryTopicKafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> orderCreatedEventTemplate() {
        return new KafkaTemplate<>(orderCreatedEventProducerFactory());
    }

    /**
     * Crea y configura un ConsumerFactory para Kafka, permitiendo la generación de consumidores
     * que reciben y deserializan mensajes de tipo OrderCreatedEvent desde el clúster de Kafka.
     * <p>
     * Funcionalidad:
     * <ul>
     *     <li>Define los parámetros de conexión y el grupo de consumidores.</li>
     *     <li>Configura la deserialización de clave y valor como String.</li>
     *     <li>Establece el reinicio automático de offset y desactiva el auto-commit.</li>
     * </ul>
     *
     * @return ConsumerFactory configurado para OrderCreatedEvent.
     */
    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> orderCreatedConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreatedEvent.class.getName());
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Proporciona un ConcurrentKafkaListenerContainerFactory para gestionar la recepción concurrente
     * de mensajes de Kafka del tipo OrderCreatedEvent.
     * <p>
     * Funcionalidad:
     * <ul>
     *     <li>Permite la gestión eficiente de múltiples hilos de consumidores.</li>
     *     <li>Utiliza el ConsumerFactory configurado para OrderCreatedEvent.</li>
     * </ul>
     *
     * @return ConcurrentKafkaListenerContainerFactory para OrderCreatedEvent.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> orderCreatedListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderCreatedConsumerFactory());
        return factory;
    }

    // ========== ORDER TOPICS ==========

    /**
     * Crea el tópico de Kafka para eventos de inventario reservado.
     * <p>
     * Funcionalidad:
     * <ul>
     *     <li>Define el nombre, número de particiones y réplicas del tópico de inventario reservado.</li>
     * </ul>
     *
     * @return NewTopic para inventario reservado.
     */
    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name(inventoryReservedTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Crea el tópico de Kafka para eventos de inventario fallido.
     * <p>
     * Funcionalidad:
     * <ul>
     *     <li>Define el nombre, número de particiones y réplicas del tópico de inventario fallido.</li>
     * </ul>
     *
     * @return NewTopic para inventario fallido.
     */
    @Bean
    public NewTopic inventoryFailedTopic() {
        return TopicBuilder.name(inventoryFailedTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

}

package com.cballestas.order_service.infrastructure.adapter.out.messaging.config;

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
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Order Topics
    @Value("${app.kafka.topics.orders-created}")
    private String ordersCreatedTopic;

    @Value("${app.kafka.topics.orders-confirmed}")
    private String ordersConfirmedTopic;

    @Value("${app.kafka.topics.orders-cancelled}")
    private String ordersCancelledTopic;

    @Value("${app.kafka.topics.orders-completed}")
    private String ordersCompletedTopic;

    @Value("${app.kafka.topics.log-error}")
    private String ordersErrorsTopic;

    @Value("${app.kafka.topics.orders-backup}")
    private String ordersBackupTopic;

    /**
     * Crea y configura un {@link ProducerFactory} para la producción de mensajes en Kafka,
     * utilizando claves y valores serializados como cadenas de texto.
     * <p>
     * Este método establece los parámetros de conexión al clúster de Kafka, habilita la idempotencia,
     * define el número de reintentos y asegura la entrega confiable de mensajes.
     *
     * @return una instancia de {@link ProducerFactory} configurada para producir mensajes de tipo String a Kafka.
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

    /**
     * Crea y configura un productor de Kafka especializado para eventos de orden,
     * serializando la clave como cadena y el valor como JSON.
     * <p>
     * Este método habilita la idempotencia, define la política de confirmación de mensajes,
     * y retorna una instancia lista para enviar eventos de tipo {@link OrderCreatedEvent}.
     *
     * @return una instancia de {@link KafkaProducer} configurada para eventos de orden.
     */
    @Bean
    public KafkaProducer<String, OrderCreatedEvent> kafkaProducer() {
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
     * Crea y configura un {@link KafkaTemplate} para el envío sencillo de mensajes a los tópicos de Kafka.
     * <p>
     * Utiliza el {@link ProducerFactory} configurado previamente para la serialización de mensajes.
     *
     * @return una instancia de {@link KafkaTemplate} para el envío de mensajes tipo String.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Crea y configura un {@link ConsumerFactory} para la recepción y deserialización de mensajes desde Kafka.
     * <p>
     * Establece los parámetros de conexión, la deserialización de claves y valores como cadenas,
     * el reinicio automático de offset y la desactivación del auto-commit.
     *
     * @return una instancia de {@link ConsumerFactory} configurada para consumidores de tipo String.
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Crea y configura un {@link ConcurrentKafkaListenerContainerFactory} para la recepción concurrente de mensajes de Kafka.
     * <p>
     * Permite la gestión eficiente de múltiples hilos de consumidores utilizando el {@link ConsumerFactory} configurado.
     *
     * @return una instancia de {@link ConcurrentKafkaListenerContainerFactory} para consumidores de Kafka tipo String.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    // ========== ORDER TOPICS ==========

    /**
     * Crea el tópico de Kafka para órdenes creadas.
     * <p>
     * Este tópico se utiliza para publicar eventos cuando una nueva orden es creada en el sistema.
     * Configura el nombre, el número de particiones y el factor de replicación según las propiedades de la aplicación.
     *
     * @return una instancia de {@link NewTopic} configurada para el tópico de órdenes creadas.
     */
    @Bean
    public NewTopic ordersCreatedTopic() {
        return TopicBuilder.name(ordersCreatedTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Crea el tópico de Kafka para órdenes confirmadas.
     * <p>
     * Este tópico se utiliza para publicar eventos cuando una orden es confirmada en el sistema.
     * Configura el nombre, el número de particiones y el factor de replicación según las propiedades de la aplicación.
     *
     * @return una instancia de {@link NewTopic} configurada para el tópico de órdenes confirmadas.
     */
    @Bean
    public NewTopic ordersConfirmedTopic() {
        return TopicBuilder.name(ordersConfirmedTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Crea el tópico de Kafka para órdenes canceladas.
     * <p>
     * Este tópico se utiliza para publicar eventos cuando una orden es cancelada en el sistema.
     * Configura el nombre, el número de particiones y el factor de replicación según las propiedades de la aplicación.
     *
     * @return una instancia de {@link NewTopic} configurada para el tópico de órdenes canceladas.
     */
    @Bean
    public NewTopic ordersCancelledTopic() {
        return TopicBuilder.name(ordersCancelledTopic)
                .partitions(2)
                .replicas(1)
                .build();
    }

    /**
     * Crea el tópico de Kafka para órdenes completadas.
     * <p>
     * Este tópico se utiliza para publicar eventos cuando una orden es completada exitosamente en el sistema.
     * Configura el nombre, el número de particiones y el factor de replicación según las propiedades de la aplicación.
     *
     * @return una instancia de {@link NewTopic} configurada para el tópico de órdenes completadas.
     */
    @Bean
    public NewTopic ordersCompletedTopic() {
        return TopicBuilder.name(ordersCompletedTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Crea el tópico de Kafka para el registro de errores relacionados con órdenes.
     * <p>
     * Este tópico se utiliza para almacenar eventos de error generados durante el procesamiento de órdenes.
     * Configura el nombre, el número de particiones y el factor de replicación según las propiedades de la aplicación.
     *
     * @return una instancia de {@link NewTopic} configurada para el tópico de errores de órdenes.
     */
    @Bean
    public NewTopic ordersErrorTopic() {
        return TopicBuilder.name(ordersErrorsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Crea el tópico de Kafka para el respaldo de eventos de órdenes.
     * <p>
     * Este tópico se utiliza para almacenar copias de respaldo de los eventos de órdenes procesados.
     * Configura el nombre, el número de particiones y el factor de replicación según las propiedades de la aplicación.
     *
     * @return una instancia de {@link NewTopic} configurada para el tópico de respaldo de órdenes.
     */
    @Bean
    public NewTopic ordersBackupTopic() {
        return TopicBuilder.name(ordersBackupTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}

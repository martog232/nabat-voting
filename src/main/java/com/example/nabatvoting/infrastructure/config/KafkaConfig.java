package com.example.nabatvoting.infrastructure.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.nabat.events.vote.VoteChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

import static com.example.nabatvoting.infrastructure.kafka.KafkaTopics.VOTE_CHANGED;

/**
 * Kafka infrastructure for the voting module.
 *
 * <p>Values on the wire are Avro with a schema id in front of them, so the producer here
 * sends plain bytes — the record was already serialised when the outbox row was written — and
 * the consumer resolves the id through the registry into the generated {@link VoteChanged}.
 * Neither side carries a copy of the schema: that is what the registry is for.
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final short topicReplicas;
    private final int topicPartitions;

    public KafkaConfig(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${nabat.schema-registry.url}") String schemaRegistryUrl,
            /*
             * Configurable, and defaulting to 1.
             *
             * These topics were created with replicas(3) while every environment in this
             * repository — docker-compose and the Helm chart alike — runs a single broker.
             * Topic creation therefore failed outright with INVALID_REPLICATION_FACTOR,
             * and with it the projection that keeps vote stats up to date. Production
             * should override this to match its real broker count.
             */
            @Value("${nabat.kafka.topic-replicas:1}") short topicReplicas,
            @Value("${nabat.kafka.topic-partitions:1}") int topicPartitions
    ) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topicReplicas = topicReplicas;
        this.topicPartitions = topicPartitions;
    }

    // ------------------------------------------------------------------- topic

    @Bean
    public NewTopic voteChangedTopic() {
        return TopicBuilder.name(VOTE_CHANGED)
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .build();
    }

    // ---------------------------------------------------------------- producer

    /**
     * The only producer, used by the outbox relay.
     *
     * <p>Its values are bytes because the relay ships what the outbox row holds. Serialising
     * at send time is precisely what the outbox moved to commit time, and with a registry
     * involved it would also put an HTTP call on the sending path.
     */
    @Bean
    public KafkaTemplate<String, byte[]> outboxKafkaTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerProperties(), new StringSerializer(), new ByteArraySerializer()));
    }

    private Map<String, Object> producerProperties() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                // Wait for all in-sync replicas, and let the client retry —
                // otherwise a transient leader election silently drops the event
                // and the projection never catches up.
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000
        );
    }

    // ---------------------------------------------------------------- consumer

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoteChanged> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, VoteChanged> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    private ConsumerFactory<String, VoteChanged> consumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Without this the deserialiser hands back a GenericRecord and every field access is
        // a string lookup that the compiler cannot check.
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(properties);
    }

    /**
     * Retries a failing record a few times, then logs it and moves on.
     *
     * <p>Without an explicit handler, an unprocessable record is retried forever and
     * blocks its partition — and with a single partition that stalls <em>all</em>
     * projection updates indefinitely. Recomputation is idempotent, so skipping a
     * poisoned event is recoverable: the next event for that alert, or an admin
     * projection rebuild, restores the correct counts.
     */
    private DefaultErrorHandler errorHandler() {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, exception) -> log.error(
                        "Giving up on record from topic {} partition {} offset {}: {}. "
                                + "The credibility projection for this alert may be stale until the next "
                                + "vote or an admin rebuild.",
                        record.topic(), record.partition(), record.offset(), exception.getMessage()),
                new FixedBackOff(1_000L, 3L)
        );
        handler.setCommitRecovered(true);
        return handler;
    }
}

package com.example.nabatvoting.infrastructure.config;

import com.example.nabatvoting.infrastructure.kafka.VoteChangedMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

import static com.example.nabatvoting.infrastructure.kafka.KafkaTopics.VOTE_CHANGED;

/**
 * Kafka infrastructure for the voting module.
 *
 * <p>Consumer and listener-container beans for the two event types are built by shared
 * generic helpers. Each type previously had its own hand-written copy of all three — six
 * near-identical beans differing only in a type parameter.
 *
 * <p>The {@link JsonMapper} is Jackson 3 ({@code tools.jackson}), which auto-registers
 * java.time support via the service loader, so {@link java.time.Instant} fields on the
 * events round-trip correctly. (The previous javadoc here claimed a module was being
 * registered explicitly; nothing was, and nothing needs to be.)
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    private final String bootstrapServers;
    private final String groupId;
    private final short topicReplicas;
    private final int topicPartitions;

    public KafkaConfig(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
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
        this.topicReplicas = topicReplicas;
        this.topicPartitions = topicPartitions;
    }

    // ------------------------------------------------------------------ topics

    @Bean
    public NewTopic voteChangedTopic() {
        return TopicBuilder.name(VOTE_CHANGED)
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .build();
    }

    // --------------------------------------------------------- shared mapper

    @Bean
    public JsonMapper kafkaObjectMapper() {
        return JsonMapper.builder().build();
    }

    // ---------------------------------------------------------------- producer

    /**
     * The one producer, used by the outbox relay.
     *
     * <p>Its values are strings because the relay sends the JSON already stored in the
     * outbox row. There used to be a template per event type, each with a Jackson
     * serializer; serialising at send time is precisely what the outbox moved to commit
     * time, and the bytes are the same either way because the same {@link JsonMapper}
     * writes them.
     */
    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerProperties(), new StringSerializer(), new StringSerializer()));
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

    // --------------------------------------------------------------- consumers

    /**
     * One factory, because there is one topic.
     *
     * <p>There were two of each — topic, factory, consumer factory, listener — for a cast and
     * a retraction. They are still two things in the domain; they are one message on one
     * keyed topic here, which is what makes everything said about an alert arrive in order.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoteChangedMessage>
            kafkaListenerContainerFactory(JsonMapper mapper) {

        ConcurrentKafkaListenerContainerFactory<String, VoteChangedMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(VoteChangedMessage.class, mapper));
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    private <T> ConsumerFactory<String, T> consumerFactory(Class<T> eventType, JsonMapper mapper) {
        JacksonJsonDeserializer<T> deserializer = new JacksonJsonDeserializer<>(eventType, mapper);
        deserializer.addTrustedPackages("com.example.nabatvoting.*");

        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(),
                deserializer
        );
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

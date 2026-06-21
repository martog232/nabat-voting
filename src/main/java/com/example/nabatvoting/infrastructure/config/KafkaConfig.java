package com.example.nabatvoting.infrastructure.config;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import org.springframework.kafka.support.serializer.JsonSerializer;

import org.springframework.kafka.annotation.EnableKafka;

import java.util.Map;

import static com.example.nabatvoting.infrastructure.kafka.KafkaTopics.VOTE_CAST;
import static com.example.nabatvoting.infrastructure.kafka.KafkaTopics.VOTE_REMOVED;

/**
 * Kafka infrastructure configuration for the voting module.
 *
 * <p>Defines a producer factory that serialises {@link VoteCastEvent}s to JSON
 * and a consumer factory that deserialises them back, along with the
 * {@code vote.cast} topic.  Both serialiser and deserialiser share a
 * dedicated {@link ObjectMapper} that has the {@link JavaTimeModule} registered
 * so that {@link java.time.Instant} fields are handled correctly.
 *
 * <p>{@link EnableKafka} activates detection of {@code @KafkaListener} methods.
 * {@link org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration} is
 * excluded from the application to prevent conflicts with these explicit beans.
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    // ------------------------------------------------------------------ topic

    @Bean
    public NewTopic voteCastTopic() {
        return TopicBuilder.name(VOTE_CAST).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic voteRemovedTopic() {
        return TopicBuilder.name(VOTE_REMOVED).partitions(1).replicas(1).build();
    }

    // --------------------------------------------------------- shared mapper

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // --------------------------------------------------------------- producer

    @Bean
    public ProducerFactory<String, VoteCastEvent> voteCastProducerFactory(ObjectMapper kafkaObjectMapper) {
        JsonSerializer<VoteCastEvent> valueSerializer = new JsonSerializer<>(kafkaObjectMapper);
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                ),
                new StringSerializer(),
                valueSerializer
        );
    }

    @Bean
    public KafkaTemplate<String, VoteCastEvent> voteCastKafkaTemplate(
            ProducerFactory<String, VoteCastEvent> voteCastProducerFactory) {
        return new KafkaTemplate<>(voteCastProducerFactory);
    }

    @Bean
    public ProducerFactory<String, VoteRemovedEvent> voteRemovedProducerFactory(ObjectMapper kafkaObjectMapper) {
        JsonSerializer<VoteRemovedEvent> valueSerializer = new JsonSerializer<>(kafkaObjectMapper);
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                ),
                new StringSerializer(),
                valueSerializer
        );
    }

    @Bean
    public KafkaTemplate<String, VoteRemovedEvent> voteRemovedKafkaTemplate(
            ProducerFactory<String, VoteRemovedEvent> voteRemovedProducerFactory) {
        return new KafkaTemplate<>(voteRemovedProducerFactory);
    }

    // --------------------------------------------------------------- consumer

    @Bean
    public ConsumerFactory<String, VoteCastEvent> voteCastConsumerFactory(ObjectMapper kafkaObjectMapper) {
        JsonDeserializer<VoteCastEvent> deserializer =
                new JsonDeserializer<>(VoteCastEvent.class, kafkaObjectMapper);
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

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoteCastEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, VoteCastEvent> voteCastConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, VoteCastEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(voteCastConsumerFactory);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, VoteRemovedEvent> voteRemovedConsumerFactory(ObjectMapper kafkaObjectMapper) {
        JsonDeserializer<VoteRemovedEvent> deserializer =
                new JsonDeserializer<>(VoteRemovedEvent.class, kafkaObjectMapper);
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

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoteRemovedEvent> voteRemovedKafkaListenerContainerFactory(
            ConsumerFactory<String, VoteRemovedEvent> voteRemovedConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, VoteRemovedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(voteRemovedConsumerFactory);
        return factory;
    }
}

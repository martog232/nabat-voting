package com.example.nabatvoting.infrastructure.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.example.nabat.events.vote.VoteChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Turns a {@link VoteChanged} into the bytes that go on the topic: a magic byte, the id of
 * the schema it was written with, then Avro.
 *
 * <h2>Why the schema is registered at startup</h2>
 * Serialisation happens inside the transaction that writes the vote — deliberately, so that
 * the bytes stored in the outbox are the bytes sent. But serialising with a registry means
 * asking it for a schema id, and the first such call is an HTTP round trip. Inside a database
 * transaction that is a network call holding a connection, and on a slow registry it is a
 * vote that times out.
 *
 * <p>So the schema is registered once at startup, through the same client the serialiser
 * uses. By the time a vote arrives the id is cached and serialising is local work.
 *
 * <p>Registering here has a second effect worth keeping. The registry refuses a schema that
 * is incompatible with what is already registered for this subject, so a change that would
 * break existing consumers fails this service at boot, loudly, instead of at the first vote
 * or — worse — silently on the consumer's side. CI catches it earlier still; this is the
 * backstop for anything deployed past it.
 */
@Component
public class VoteEventAvroSerializer {

    /** What {@code TopicNameStrategy} calls the schema for this topic's values. */
    static final String SUBJECT = KafkaTopics.VOTE_CHANGED + "-value";

    private static final Logger log = LoggerFactory.getLogger(VoteEventAvroSerializer.class);

    private final SchemaRegistryClient registryClient;
    private final KafkaAvroSerializer serializer;

    public VoteEventAvroSerializer(@Value("${nabat.schema-registry.url}") String registryUrl,
                                   @Value("${nabat.schema-registry.cache-size:100}") int cacheSize) {
        Map<String, Object> config = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);

        // Through the factory rather than `new CachedSchemaRegistryClient`, because the
        // factory is what understands a `mock://` url — the in-memory registry the tests run
        // against, and the only reason they need no fifth container.
        this.registryClient = SchemaRegistryClientFactory.newClient(
                registryUrl, cacheSize, List.of(new AvroSchemaProvider()), config, Map.of());

        // Given the client, the serialiser looks its schema id up in that client's cache
        // rather than opening its own connection — which is what makes the startup
        // registration below enough to keep HTTP out of the vote's transaction.
        this.serializer = new KafkaAvroSerializer(registryClient, config);
    }

    public byte[] serialize(VoteChanged event) {
        return serializer.serialize(KafkaTopics.VOTE_CHANGED, event);
    }

    @EventListener(ApplicationReadyEvent.class)
    void registerSchemaBeforeTheFirstVote() {
        try {
            int id = registryClient.register(SUBJECT, new AvroSchema(VoteChanged.getClassSchema()));
            log.info("Registered {} as schema id {}", SUBJECT, id);
        } catch (Exception e) {
            // Fatal on purpose. A rejected schema means this build's events cannot be read by
            // consumers already out there, and starting anyway would publish them regardless.
            throw new IllegalStateException(
                    "Could not register " + SUBJECT + " with the schema registry", e);
        }
    }
}

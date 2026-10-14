package com.example.nabatvoting.infrastructure.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.example.nabat.events.vote.VoteChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
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
 *
 * <h2>Refused and unreachable are not the same failure</h2>
 * Only the first is fatal. A registry that answers "no" has judged this build's schema, and
 * starting anyway would publish messages nobody can read. A registry that does not answer has
 * judged nothing, and a producer that exits over it is a service killed by a dependency it
 * needs once — so that case is a warning and another attempt on the schedule below. Casting a
 * vote fails in the meantime, since serialising one needs the id; everything else this service
 * does carries on, and it recovers without help.
 */
@Component
public class VoteEventAvroSerializer {

    /** What {@code TopicNameStrategy} calls the schema for this topic's values. */
    static final String SUBJECT = KafkaTopics.VOTE_CHANGED + "-value";

    private static final Logger log = LoggerFactory.getLogger(VoteEventAvroSerializer.class);

    private final SchemaRegistryClient registryClient;
    private final KafkaAvroSerializer serializer;

    /** Set once the registry has accepted the schema; the retry stops there. */
    private volatile boolean registered;

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
    @Scheduled(fixedDelayString = "${nabat.schema-registry.retry-interval:PT30S}")
    void registerSchemaBeforeTheFirstVote() {
        if (registered) {
            return;
        }

        try {
            int id = registryClient.register(SUBJECT, new AvroSchema(VoteChanged.getClassSchema()));
            registered = true;
            log.info("Registered {} as schema id {}", SUBJECT, id);
        } catch (RestClientException e) {
            // The registry answered, and the answer was no. That is a verdict on this build's
            // schema — consumers already out there could not read what it would publish — so
            // starting anyway would mean publishing them regardless. Fatal on purpose.
            throw new IllegalStateException(
                    SUBJECT + " was rejected by the schema registry: " + e.getMessage(), e);
        } catch (IOException e) {
            // The registry did not answer, which is a verdict on nothing. Retrying beats
            // exiting: casting a vote does fail meanwhile — serialising one needs the id this
            // call would have cached — but reads, health and every other endpoint keep
            // working, and the service recovers by itself instead of crash-looping until a
            // human notices.
            log.warn("Schema registry unreachable ({}); {} not registered yet, so votes will "
                     + "fail until it answers. Retrying.", e.getMessage(), SUBJECT);
        }
    }
}

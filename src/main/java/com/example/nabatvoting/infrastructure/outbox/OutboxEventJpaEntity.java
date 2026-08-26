package com.example.nabatvoting.infrastructure.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * One event waiting to be, or already, published.
 *
 * <p>The payload is stored as the serialised form that will go on the wire, not as the
 * event object. Reconstructing it at send time would mean a payload that serialises today
 * but fails to deserialise after a class changes turns every pending row into a permanent
 * failure — the exact trap nabat-app's Event Publication Registry has, discoverable only
 * after a crash leaves rows behind to replay.
 */
@Entity
@Table(name = "outbox_event")
@Getter
@NoArgsConstructor
public class OutboxEventJpaEntity {

    /** How much of a failure is worth keeping. Enough to identify it, not to hold a stack trace. */
    private static final int MAX_ERROR_LENGTH = 2000;

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "partition_key", nullable = false)
    private String partitionKey;

    @Column(name = "payload", nullable = false, length = 8192)
    private String payload;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "last_error", length = MAX_ERROR_LENGTH)
    private String lastError;

    private OutboxEventJpaEntity(UUID id, String topic, String eventType, String partitionKey,
                                 String payload, Instant occurredAt) {
        this.id = id;
        this.topic = topic;
        this.eventType = eventType;
        this.partitionKey = partitionKey;
        this.payload = payload;
        this.occurredAt = occurredAt;
    }

    static OutboxEventJpaEntity pending(String topic, String eventType, String partitionKey,
                                        String payload, Instant occurredAt) {
        return new OutboxEventJpaEntity(UUID.randomUUID(), topic, eventType, partitionKey,
                payload, occurredAt);
    }

    void markPublished(Instant publishedAt) {
        this.publishedAt = publishedAt;
        this.lastError = null;
    }

    /**
     * Records a failed send. The row stays pending on purpose: an outbox that gives up is a
     * dual write with extra steps. {@code attempts} and {@code lastError} exist so that a row
     * failing forever is visible rather than silent.
     */
    void markFailed(Throwable failure) {
        this.attempts++;
        String message = failure.toString();
        this.lastError = message.length() > MAX_ERROR_LENGTH
                ? message.substring(0, MAX_ERROR_LENGTH)
                : message;
    }
}

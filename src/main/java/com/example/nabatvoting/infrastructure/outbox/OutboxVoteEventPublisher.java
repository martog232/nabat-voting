package com.example.nabatvoting.infrastructure.outbox;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import com.example.nabatvoting.infrastructure.kafka.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

/**
 * Writes vote events to the outbox table instead of sending them to Kafka.
 *
 * <p>This runs inside the caller's transaction — the same one that writes the vote — which
 * is the whole point: the event and the vote it describes commit together, or neither does.
 * {@link OutboxRelay} does the sending afterwards, from committed rows only.
 *
 * <p>Serialisation happens here rather than in the relay, with the same {@link JsonMapper}
 * the consumer deserialises with, so the bytes stored are the bytes sent. That also means a
 * failure to serialise fails the vote — loudly, at the point of the mistake — instead of
 * being discovered later by a relay that cannot drain its backlog.
 */
@Component
public class OutboxVoteEventPublisher implements VoteEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxVoteEventPublisher.class);

    private final OutboxEventJpaRepository outbox;
    private final JsonMapper mapper;

    public OutboxVoteEventPublisher(OutboxEventJpaRepository outbox, JsonMapper mapper) {
        this.outbox = outbox;
        this.mapper = mapper;
    }

    @Override
    public void publish(VoteCastEvent event) {
        append(KafkaTopics.VOTE_CAST, VoteCastEvent.class.getSimpleName(),
                event.alertId(), event, event.castAt());
    }

    @Override
    public void publishRemoved(VoteRemovedEvent event) {
        append(KafkaTopics.VOTE_REMOVED, VoteRemovedEvent.class.getSimpleName(),
                event.alertId(), event, event.removedAt());
    }

    private void append(String topic, String eventType, String alertId, Object event, Instant occurredAt) {
        outbox.save(OutboxEventJpaEntity.pending(
                topic, eventType, alertId, mapper.writeValueAsString(event), occurredAt));

        log.debug("Queued {} for alert '{}' to topic '{}'", eventType, alertId, topic);
    }
}

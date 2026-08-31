package com.example.nabatvoting.infrastructure.outbox;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import com.example.nabatvoting.infrastructure.kafka.KafkaTopics;
import com.example.nabatvoting.infrastructure.kafka.VoteChangedEvents;
import com.example.nabatvoting.infrastructure.kafka.VoteEventAvroSerializer;
import org.example.nabat.events.vote.VoteChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Writes vote events to the outbox table instead of sending them to Kafka.
 *
 * <p>This runs inside the caller's transaction — the same one that writes the vote — which
 * is the whole point: the event and the vote it describes commit together, or neither does.
 * {@link OutboxRelay} does the sending afterwards, from committed rows only.
 *
 * <p>Serialisation happens here rather than in the relay, so the bytes stored are the bytes
 * sent, schema id and all. A payload rebuilt at send time could fail to serialise after a
 * schema change and turn every pending row into a permanent failure; this way a serialisation
 * problem fails the vote, at the point of the mistake. It is local work — the schema id is
 * cached at startup by {@link VoteEventAvroSerializer}, so no HTTP call happens inside this
 * transaction.
 *
 * <p>Both domain events become one {@link VoteChanged} on one topic. The translation belongs
 * here: this is the boundary where a domain fact turns into somebody else's input, and the
 * reason for a single topic — order per alert — is a transport concern the domain should not
 * have to know about.
 */
@Component
public class OutboxVoteEventPublisher implements VoteEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxVoteEventPublisher.class);

    private final OutboxEventJpaRepository outbox;
    private final VoteEventAvroSerializer serializer;

    public OutboxVoteEventPublisher(OutboxEventJpaRepository outbox,
                                    VoteEventAvroSerializer serializer) {
        this.outbox = outbox;
        this.serializer = serializer;
    }

    @Override
    public void publish(VoteCastEvent event) {
        append(VoteChangedEvents.from(event));
    }

    @Override
    public void publishRemoved(VoteRemovedEvent event) {
        append(VoteChangedEvents.from(event));
    }

    private void append(VoteChanged event) {
        outbox.save(OutboxEventJpaEntity.pending(
                KafkaTopics.VOTE_CHANGED,
                event.getChangeType().name(),
                // The key, and so the partition: everything about one alert stays in order.
                event.getAlertId(),
                serializer.serialize(event),
                event.getOccurredAt()));

        log.debug("Queued {} for alert '{}' to topic '{}'",
                event.getChangeType(), event.getAlertId(), KafkaTopics.VOTE_CHANGED);
    }
}

package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.event.VoteTallies;
import com.example.nabatvoting.domain.model.VoteType;

import java.time.Instant;
import java.util.UUID;

/**
 * What goes on the wire for {@link KafkaTopics#VOTE_CHANGED}.
 *
 * <p>The domain keeps two events, because casting a vote and retracting one are two
 * different things to say. The transport carries one message, because the ordering guarantee
 * that matters — everything about an alert in the order it happened — is a property of a
 * single keyed topic, and two topics cannot give it.
 *
 * <p>That split is the point of this type: the domain is not bent to fit Kafka, and the
 * topic is not shaped by how many domain events happen to exist. {@code changeType} is what
 * a consumer reads to tell them apart; {@code voteId} and {@code voteType} are absent on a
 * retraction, which describes an alert's votes rather than any one vote.
 */
public record VoteChangedMessage(
        ChangeType changeType,
        UUID voteId,
        String alertId,
        String voterId,
        VoteType voteType,
        Instant occurredAt,
        VoteTallies tallies
) {

    public enum ChangeType { CAST, REMOVED }

    public static VoteChangedMessage of(VoteCastEvent event) {
        return new VoteChangedMessage(
                ChangeType.CAST,
                event.voteId(),
                event.alertId(),
                event.voterId(),
                event.voteType(),
                event.castAt(),
                event.tallies()
        );
    }

    public static VoteChangedMessage of(VoteRemovedEvent event) {
        return new VoteChangedMessage(
                ChangeType.REMOVED,
                null,
                event.alertId(),
                event.voterId(),
                null,
                event.removedAt(),
                event.tallies()
        );
    }
}

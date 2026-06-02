package com.example.nabatvoting.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Domain event raised when a vote is cast on an alert.
 *
 * <p>This event is serialised to JSON and published to the Kafka topic
 * {@code vote.cast}.  Downstream consumers (e.g. the credibility projection
 * updater) subscribe to the same topic to react to new votes.
 */
public record VoteCastEvent(
        UUID voteId,
        String alertId,
        String voterId,
        boolean positive,
        Instant castAt
) {
    public static VoteCastEvent of(UUID voteId, String alertId, String voterId,
                                   boolean positive, Instant castAt) {
        return new VoteCastEvent(voteId, alertId, voterId, positive, castAt);
    }
}

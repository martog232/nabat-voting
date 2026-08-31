package com.example.nabatvoting.domain.event;

import java.time.Instant;

/**
 * Emitted when a voter retracts their vote on an alert.
 *
 * <p>Carries the alert's {@link VoteTallies} after the removal, for the same reason
 * {@link VoteCastEvent} does: a consumer with its own projection writes what the event says
 * rather than deriving it. This service's own consumer ignores them and recomputes from the
 * write model — it can, being the owner of that model.
 */
public record VoteRemovedEvent(
        String alertId,
        String voterId,
        Instant removedAt,
        VoteTallies tallies
) {
    public static VoteRemovedEvent of(String alertId, String voterId, Instant removedAt,
                                      VoteTallies tallies) {
        return new VoteRemovedEvent(alertId, voterId, removedAt, tallies);
    }
}

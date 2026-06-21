package com.example.nabatvoting.domain.event;

import java.time.Instant;

/**
 * Emitted when a voter retracts their vote on an alert. Carries only what the
 * projection needs to react: the affected alert. Downstream consumers recompute
 * that alert's read-model from the write-model, so no vote type is required.
 */
public record VoteRemovedEvent(
        String alertId,
        String voterId,
        Instant removedAt
) {
    public static VoteRemovedEvent of(String alertId, String voterId, Instant removedAt) {
        return new VoteRemovedEvent(alertId, voterId, removedAt);
    }
}

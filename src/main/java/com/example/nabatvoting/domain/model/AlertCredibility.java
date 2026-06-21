package com.example.nabatvoting.domain.model;

import java.util.Objects;

/**
 * Read-model value object holding the aggregated vote counts for a single alert.
 *
 * <p>This is the materialised projection that backs the {@code votes/stats}
 * endpoint. The credibility score is derived from the counts using the same
 * formula as the write side, so the score is never stored — only the raw counts
 * are persisted, which keeps the projection trivially rebuildable.
 */
public record AlertCredibility(AlertId alertId, int upvotes, int downvotes, int confirmations) {

    public AlertCredibility {
        Objects.requireNonNull(alertId, "alertId must not be null");
    }

    /** Empty projection for an alert that has not received any votes yet. */
    public static AlertCredibility empty(AlertId alertId) {
        return new AlertCredibility(alertId, 0, 0, 0);
    }

    public int credibilityScore() {
        return upvotes - downvotes + (confirmations * 2);
    }
}

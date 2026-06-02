package com.example.nabatvoting.domain.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Vote aggregate root.
 *
 * <p>A vote expresses whether a voter considers an alert credible ({@code positive=true})
 * or not ({@code positive=false}).
 */
public final class Vote {

    private final VoteId id;
    private final AlertId alertId;
    private final VoterId voterId;
    private final boolean positive;
    private final Instant castAt;

    public Vote(VoteId id, AlertId alertId, VoterId voterId, boolean positive, Instant castAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.alertId = Objects.requireNonNull(alertId, "alertId must not be null");
        this.voterId = Objects.requireNonNull(voterId, "voterId must not be null");
        this.positive = positive;
        this.castAt = Objects.requireNonNull(castAt, "castAt must not be null");
    }

    public VoteId getId() {
        return id;
    }

    public AlertId getAlertId() {
        return alertId;
    }

    public VoterId getVoterId() {
        return voterId;
    }

    public boolean isPositive() {
        return positive;
    }

    public Instant getCastAt() {
        return castAt;
    }
}

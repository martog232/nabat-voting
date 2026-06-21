package com.example.nabatvoting.domain.model;

import lombok.Getter;

import java.time.Instant;
import java.util.Objects;

@Getter
public final class Vote {

    private final VoteId id;
    private final AlertId alertId;
    private final VoterId voterId;
    private final VoteType voteType;
    private final Instant castAt;

    public Vote(VoteId id, AlertId alertId, VoterId voterId, VoteType voteType, Instant castAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.alertId = Objects.requireNonNull(alertId, "alertId must not be null");
        this.voterId = Objects.requireNonNull(voterId, "voterId must not be null");
        this.voteType = Objects.requireNonNull(voteType, "voteType must not be null");
        this.castAt = Objects.requireNonNull(castAt, "castAt must not be null");
    }

}

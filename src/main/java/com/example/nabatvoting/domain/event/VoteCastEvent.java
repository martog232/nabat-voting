package com.example.nabatvoting.domain.event;

import com.example.nabatvoting.domain.model.VoteType;

import java.time.Instant;
import java.util.UUID;

public record VoteCastEvent(
        UUID voteId,
        String alertId,
        String voterId,
        VoteType voteType,
        Instant castAt
) {
    public static VoteCastEvent of(UUID voteId, String alertId, String voterId,
                                   VoteType voteType, Instant castAt) {
        return new VoteCastEvent(voteId, alertId, voterId, voteType, castAt);
    }
}

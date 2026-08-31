package com.example.nabatvoting.domain.event;

import com.example.nabatvoting.domain.model.VoteType;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted when a vote is cast or changed.
 *
 * <p>Carries {@link VoteTallies} — the alert's counts as of this vote, read from the write
 * model inside the transaction that wrote it. Without them a consumer maintaining its own
 * projection has two bad options: apply a delta, which double-counts on redelivery, or call
 * back for the stats, which is a synchronous hop onto an asynchronously-updated projection.
 * With them, applying the event is an absolute write and therefore idempotent.
 */
public record VoteCastEvent(
        UUID voteId,
        String alertId,
        String voterId,
        VoteType voteType,
        Instant castAt,
        VoteTallies tallies
) {
    public static VoteCastEvent of(UUID voteId, String alertId, String voterId,
                                   VoteType voteType, Instant castAt, VoteTallies tallies) {
        return new VoteCastEvent(voteId, alertId, voterId, voteType, castAt, tallies);
    }
}

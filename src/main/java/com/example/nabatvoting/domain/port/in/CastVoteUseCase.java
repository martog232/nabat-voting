package com.example.nabatvoting.domain.port.in;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteCounts;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoteType;
import com.example.nabatvoting.domain.model.VoterId;

import java.time.Instant;
import java.util.Optional;

public interface CastVoteUseCase {

    CastVoteResult castVote(CastVoteCommand command);

    /**
     * Removes the voter's vote and returns the resulting tallies.
     *
     * <p>Returns the counts (rather than {@code void}) for the same read-your-writes
     * reason as {@link #castVote}: the caller needs the post-removal state without
     * waiting for the projection.
     */
    VoteStats removeVote(AlertId alertId, VoterId voterId);

    /** The voter's current vote on the alert, or empty if they have not voted. */
    Optional<VoteType> getUserVote(AlertId alertId, VoterId voterId);

    /**
     * Aggregate stats for an alert, served from the {@code alert_credibility}
     * projection. Eventually consistent — see {@code CredibilityProjectionUpdater}.
     * Callers that just mutated a vote should use the stats returned by
     * {@link #castVote}/{@link #removeVote} instead.
     */
    VoteStats getVoteStats(AlertId alertId);

    /**
     * Outcome of casting a vote.
     *
     * @param voteId  the stored vote's id
     * @param created {@code true} if a new vote was recorded (201 Created),
     *                {@code false} if an existing vote was changed to a new type (200 OK)
     * @param castAt  when the vote was recorded — the persisted timestamp, not a
     *                timestamp invented by the controller
     * @param stats   tallies recomputed from the write model inside the same
     *                transaction, so the caller can see its own vote immediately
     */
    record CastVoteResult(VoteId voteId, boolean created, Instant castAt, VoteStats stats) {}

    record VoteStats(
            int upvotes,
            int downvotes,
            int confirmations,
            int credibilityScore
    ) {
        public static VoteStats from(VoteCounts counts) {
            return new VoteStats(
                    counts.upvotes(),
                    counts.downvotes(),
                    counts.confirmations(),
                    // Single formula authority — see VoteCounts.
                    counts.credibilityScore()
            );
        }
    }
}

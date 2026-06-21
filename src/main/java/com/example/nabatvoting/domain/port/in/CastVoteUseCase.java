package com.example.nabatvoting.domain.port.in;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoteType;
import com.example.nabatvoting.domain.model.VoterId;

import java.util.Optional;

public interface CastVoteUseCase {

    CastVoteResult castVote(CastVoteCommand command);

    void removeVote(AlertId alertId, VoterId voterId);

    /** The voter's current vote on the alert, or empty if they have not voted. */
    Optional<VoteType> getUserVote(AlertId alertId, VoterId voterId);

    VoteStats getVoteStats(AlertId alertId);

    /**
     * Outcome of casting a vote.
     *
     * @param voteId  the stored vote's id
     * @param created {@code true} if a new vote was recorded (201 Created),
     *                {@code false} if an existing vote was changed to a new type (200 OK)
     */
    record CastVoteResult(VoteId voteId, boolean created) {}

    record VoteStats(
            int upvotes,
            int downvotes,
            int confirmations,
            int credibilityScore
    ) {
        public static VoteStats fromCounts(int upvotes, int downvotes, int confirmations) {
            int credibilityScore = upvotes - downvotes + (confirmations * 2);
            return new VoteStats(upvotes, downvotes, confirmations, credibilityScore);
        }
    }
}

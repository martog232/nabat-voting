package com.example.nabatvoting.domain.port.in;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoterId;

public interface CastVoteUseCase {

    VoteId castVote(CastVoteCommand command);

    void removeVote(AlertId alertId, VoterId voterId);

    boolean hasUserVoted(AlertId alertId, VoterId voterId);

    VoteStats getVoteStats(AlertId alertId);

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

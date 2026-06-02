package com.example.nabatvoting.domain.port.in;

import com.example.nabatvoting.domain.model.VoteId;

/**
 * Inbound port: use case for casting a vote on an alert.
 */
public interface CastVoteUseCase {

    /**
     * Casts a vote as described by the {@code command}.
     *
     * @param command the cast-vote command
     * @return the identifier of the newly created vote
     */
    VoteId castVote(CastVoteCommand command);
}

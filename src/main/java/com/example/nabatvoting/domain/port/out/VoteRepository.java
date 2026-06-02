package com.example.nabatvoting.domain.port.out;

import com.example.nabatvoting.domain.model.Vote;

/**
 * Outbound port: persists and loads votes.
 */
public interface VoteRepository {

    /**
     * Persists a new vote.
     *
     * @param vote the vote to save
     */
    void save(Vote vote);
}

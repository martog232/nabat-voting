package com.example.nabatvoting.domain.port.out;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoteCounts;
import com.example.nabatvoting.domain.model.VoterId;

import java.util.List;
import java.util.Optional;

public interface VoteRepository {

    void save(Vote vote);

    void deleteByAlertIdAndVoterId(AlertId alertId, VoterId voterId);

    Optional<Vote> findByAlertIdAndVoterId(AlertId alertId, VoterId voterId);

    /**
     * Current tallies straight from the write model, in a single query.
     *
     * <p>Reading the write model — rather than the asynchronously-maintained
     * {@code alert_credibility} projection — is what lets a caller see its own
     * vote reflected immediately.
     */
    VoteCounts countsFor(AlertId alertId);

    /** Every alert that has at least one vote — used to rebuild the projection. */
    List<AlertId> findDistinctAlertIds();
}

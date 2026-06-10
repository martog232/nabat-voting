package com.example.nabatvoting.domain.port.out;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoterId;

import java.util.Optional;

public interface VoteRepository {

    void save(Vote vote);

    void deleteByAlertIdAndVoterId(AlertId alertId, VoterId voterId);

    Optional<Vote> findByAlertIdAndVoterId(AlertId alertId, VoterId voterId);

    int countUpvotes(AlertId alertId);

    int countDownvotes(AlertId alertId);

    int countConfirmations(AlertId alertId);
}

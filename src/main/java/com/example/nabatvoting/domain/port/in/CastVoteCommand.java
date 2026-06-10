package com.example.nabatvoting.domain.port.in;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteType;
import com.example.nabatvoting.domain.model.VoterId;

import java.util.Objects;

public record CastVoteCommand(AlertId alertId, VoterId voterId, VoteType voteType) {

    public CastVoteCommand {
        Objects.requireNonNull(alertId, "alertId must not be null");
        Objects.requireNonNull(voterId, "voterId must not be null");
        Objects.requireNonNull(voteType, "voteType must not be null");
    }
}

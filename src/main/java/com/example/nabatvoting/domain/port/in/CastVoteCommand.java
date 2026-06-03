package com.example.nabatvoting.domain.port.in;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoterId;

import java.util.Objects;

/**
 * Command object that carries the intent to cast a vote on an alert.
 */
public record CastVoteCommand(AlertId alertId, VoterId voterId, boolean positive) {

    public CastVoteCommand {
        Objects.requireNonNull(alertId, "alertId must not be null");
        Objects.requireNonNull(voterId, "voterId must not be null");
    }
}

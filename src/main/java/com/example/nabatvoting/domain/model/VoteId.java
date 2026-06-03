package com.example.nabatvoting.domain.model;

import java.util.Objects;
import java.util.UUID;

public record VoteId(UUID value) {

    public VoteId {
        Objects.requireNonNull(value, "VoteId value must not be null");
    }

    public static VoteId generate() {
        return new VoteId(UUID.randomUUID());
    }
}

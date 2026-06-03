package com.example.nabatvoting.domain.model;

import java.util.Objects;

public record VoterId(String value) {

    public VoterId {
        Objects.requireNonNull(value, "VoterId value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("VoterId value must not be blank");
        }
    }
}

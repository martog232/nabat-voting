package com.example.nabatvoting.domain.model;

import java.util.Objects;

public record AlertId(String value) {

    public AlertId {
        Objects.requireNonNull(value, "AlertId value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("AlertId value must not be blank");
        }
    }
}

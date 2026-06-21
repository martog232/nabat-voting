package com.example.nabatvoting.domain.port.out;

import com.example.nabatvoting.domain.model.AlertCredibility;
import com.example.nabatvoting.domain.model.AlertId;

import java.util.Optional;

/**
 * Outbound port for persisting and reading the credibility read-model.
 *
 * <p>Writes are upserts keyed by {@code alertId}: saving an {@link AlertCredibility}
 * replaces any existing row, which makes projection updates idempotent.
 */
public interface CredibilityProjectionStore {

    /** Inserts or replaces the projection row for {@code credibility.alertId()}. */
    void save(AlertCredibility credibility);

    Optional<AlertCredibility> findByAlertId(AlertId alertId);
}

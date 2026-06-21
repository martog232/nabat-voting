package com.example.nabatvoting.domain.port.in;

/**
 * Inbound port for rebuilding the credibility read-model from the {@code votes}
 * write-model. This is the CQRS "replay" operation: because the projection is
 * derived state, it can always be discarded and recomputed from the source of
 * truth (e.g. after a bug in the projection logic, or when bootstrapping a new
 * read-model).
 */
public interface RebuildCredibilityProjectionUseCase {

    /**
     * Recomputes the projection for every alert that has at least one vote.
     *
     * @return the number of alerts whose projection was rebuilt
     */
    int rebuildAll();
}

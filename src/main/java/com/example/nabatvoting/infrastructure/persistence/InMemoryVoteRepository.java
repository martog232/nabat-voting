package com.example.nabatvoting.infrastructure.persistence;

import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory implementation of {@link VoteRepository}.
 *
 * <p>Suitable for local development and testing.  A production deployment
 * would replace this with a database-backed adapter (e.g. JPA/R2DBC).
 */
@Repository
public class InMemoryVoteRepository implements VoteRepository {

    private final List<Vote> store = new CopyOnWriteArrayList<>();

    @Override
    public void save(Vote vote) {
        store.add(vote);
    }

    /**
     * Returns a read-only snapshot of all persisted votes.
     * Useful for testing and diagnostics.
     */
    public List<Vote> findAll() {
        return Collections.unmodifiableList(new ArrayList<>(store));
    }
}

package com.example.nabatvoting.infrastructure.persistence;

import com.example.nabatvoting.domain.model.AlertCredibility;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.port.out.CredibilityProjectionStore;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

@Component
public class CredibilityProjectionStoreAdapter implements CredibilityProjectionStore {

    private final AlertCredibilityJpaRepository jpaRepository;

    public CredibilityProjectionStoreAdapter(AlertCredibilityJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public void save(AlertCredibility credibility) {
        // save() on a PK-keyed entity is an upsert (Hibernate merge), which keeps
        // projection writes idempotent.
        jpaRepository.save(new AlertCredibilityJpaEntity(
                credibility.alertId().value(),
                credibility.upvotes(),
                credibility.downvotes(),
                credibility.confirmations(),
                Instant.now()
        ));
    }

    @Override
    public Optional<AlertCredibility> findByAlertId(AlertId alertId) {
        return jpaRepository.findById(alertId.value())
                .map(entity -> new AlertCredibility(
                        new AlertId(entity.getAlertId()),
                        entity.getUpvotes(),
                        entity.getDownvotes(),
                        entity.getConfirmations()
                ));
    }
}

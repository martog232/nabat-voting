package com.example.nabatvoting.infrastructure.persistence;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Creates an alert's lock row in a transaction of its own.
 *
 * <p>Separate bean and {@code REQUIRES_NEW} for one reason: two first-ever votes on the same
 * alert race to insert this row, and the loser gets a unique-key violation. In PostgreSQL a
 * constraint violation aborts the whole transaction — every statement after it fails with
 * "current transaction is aborted" — so letting that happen inside the vote's own transaction
 * would leave that transaction unusable. Here it costs only this inner transaction, which had
 * nothing else in it. Self-invocation would bypass the proxy and give none of this.
 *
 * <p><b>The violation is deliberately not caught here.</b> Catching it inside the transactional
 * method lets the method return normally, and the interceptor then tries to commit a
 * transaction Spring has already marked rollback-only — which fails with
 * {@code UnexpectedRollbackException} and takes the caller down anyway. The exception has to
 * cross the transaction boundary before anyone swallows it, so {@link AlertVoteLockAdapter}
 * is where it is handled.
 */
@Component
public class AlertVoteLockRowCreator {

    private final AlertVoteLockJpaRepository repository;

    public AlertVoteLockRowCreator(AlertVoteLockJpaRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void create(String alertId) {
        repository.saveAndFlush(new AlertVoteLockJpaEntity(alertId));
    }
}

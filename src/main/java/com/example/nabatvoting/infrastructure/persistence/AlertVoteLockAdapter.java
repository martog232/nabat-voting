package com.example.nabatvoting.infrastructure.persistence;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.port.out.AlertVoteLock;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.UnexpectedRollbackException;

@Component
public class AlertVoteLockAdapter implements AlertVoteLock {

    private final AlertVoteLockJpaRepository repository;
    private final AlertVoteLockRowCreator rowCreator;

    public AlertVoteLockAdapter(AlertVoteLockJpaRepository repository,
                                AlertVoteLockRowCreator rowCreator) {
        this.repository = repository;
        this.rowCreator = rowCreator;
    }

    /**
     * Take the lock; create the row first if this alert has never been voted on.
     *
     * <p>The second {@code lockByAlertId} is not a retry of a failure — it is the lock itself.
     * The first call only answers whether the row exists, and cannot lock a row that does not.
     * On the common path — an alert that already has votes — the first call takes the lock and
     * the rest is skipped.
     *
     * <p>Losing the insert race is the expected outcome for one of two simultaneous first
     * votes, not an error: the row it collided with is the row this method wanted, committed
     * by the winner. It is caught here rather than inside {@link AlertVoteLockRowCreator}
     * because a transactional method cannot swallow its own rollback — see the comment there.
     * Both exceptions mean the same thing, and which one surfaces depends on whether the
     * violation is detected at flush or at commit.
     */
    @Override
    public void acquire(AlertId alertId) {
        if (repository.lockByAlertId(alertId.value()).isPresent()) {
            return;
        }
        try {
            rowCreator.create(alertId.value());
        } catch (DataIntegrityViolationException | UnexpectedRollbackException lostTheRace) {
            // The winner's row is committed; the lock below is what actually matters.
        }
        repository.lockByAlertId(alertId.value());
    }
}

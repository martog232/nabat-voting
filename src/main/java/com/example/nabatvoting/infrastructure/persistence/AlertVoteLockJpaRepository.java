package com.example.nabatvoting.infrastructure.persistence;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface AlertVoteLockJpaRepository extends JpaRepository<AlertVoteLockJpaEntity, String> {

    /**
     * {@code SELECT ... FOR UPDATE} on the alert's lock row.
     *
     * <p>Written out rather than relying on {@code findById}, because the lock mode has to be
     * on the query for the {@code FOR UPDATE} to be emitted at all — and a silently missing
     * {@code FOR UPDATE} is a lock that looks taken and holds nothing.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT l FROM AlertVoteLockJpaEntity l WHERE l.alertId = :alertId")
    Optional<AlertVoteLockJpaEntity> lockByAlertId(@Param("alertId") String alertId);
}

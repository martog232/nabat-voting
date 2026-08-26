package com.example.nabatvoting.infrastructure.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxEventJpaRepository extends JpaRepository<OutboxEventJpaEntity, UUID> {

    /**
     * Claims the oldest pending rows for this relay and this transaction.
     *
     * <p>{@code SKIP LOCKED} — the {@code -2} lock timeout — is what lets a second replica
     * run the relay at the same time without either waiting on the other or sending the same
     * row twice. Where the dialect cannot express it, this degrades to a plain
     * {@code FOR UPDATE}: the replicas serialise, which is slower but never wrong.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select e from OutboxEventJpaEntity e where e.publishedAt is null order by e.occurredAt asc")
    List<OutboxEventJpaEntity> lockPending(Pageable pageable);

    @Modifying
    @Query("delete from OutboxEventJpaEntity e where e.publishedAt is not null and e.publishedAt < :before")
    int deletePublishedBefore(@Param("before") Instant before);
}

package com.example.nabatvoting.infrastructure.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * A row to lock, and nothing else — the table has no columns beyond its key on purpose.
 *
 * <p>It exists as an entity and not only as a migration because the tests build their schema
 * from the entities rather than from Flyway.
 */
@Entity
@Table(name = "alert_vote_lock")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class AlertVoteLockJpaEntity {

    @Id
    @Column(name = "alert_id")
    private String alertId;
}

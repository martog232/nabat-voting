package com.example.nabatvoting.infrastructure.persistence;

import com.example.nabatvoting.domain.model.VoteType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface VoteJpaRepository extends JpaRepository<VoteJpaEntity, UUID> {

    Optional<VoteJpaEntity> findByAlertIdAndVoterId(String alertId, String voterId);

    void deleteByAlertIdAndVoterId(String alertId, String voterId);

    @Query("SELECT DISTINCT v.alertId FROM VoteJpaEntity v")
    List<String> findDistinctAlertIds();

    /**
     * All three tallies in one round-trip. Replaces the three separate
     * {@code COUNT(*)} queries this repository used to expose, which meant every
     * projection recompute — i.e. every vote event — issued three statements
     * where one suffices.
     *
     * <p>Vote types with no rows are simply absent from the result; the caller
     * defaults them to zero.
     */
    @Query("""
        SELECT v.voteType AS voteType, COUNT(v) AS total
        FROM VoteJpaEntity v
        WHERE v.alertId = :alertId
        GROUP BY v.voteType
        """)
    List<VoteTypeTally> tallyByAlertId(@Param("alertId") String alertId);

    /** Projection for {@link #tallyByAlertId(String)}. */
    interface VoteTypeTally {
        VoteType getVoteType();

        long getTotal();
    }
}

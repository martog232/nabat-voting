package com.example.nabatvoting.infrastructure.persistence;

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

    @Query("SELECT COUNT(v) FROM VoteJpaEntity v WHERE v.alertId = :alertId AND v.voteType = 'UPVOTE'")
    int countUpvotesByAlertId(@Param("alertId") String alertId);

    @Query("SELECT COUNT(v) FROM VoteJpaEntity v WHERE v.alertId = :alertId AND v.voteType = 'DOWNVOTE'")
    int countDownvotesByAlertId(@Param("alertId") String alertId);

    @Query("SELECT COUNT(v) FROM VoteJpaEntity v WHERE v.alertId = :alertId AND v.voteType = 'CONFIRM'")
    int countConfirmationsByAlertId(@Param("alertId") String alertId);
}

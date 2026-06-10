package com.example.nabatvoting.infrastructure.persistence;

import com.example.nabatvoting.domain.model.VoteType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "votes")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class VoteJpaEntity {

    @Id
    private UUID id;

    @Column(name = "alert_id", nullable = false)
    private String alertId;

    @Column(name = "voter_id", nullable = false)
    private String voterId;

    @Enumerated(EnumType.STRING)
    @Column(name = "vote_type", nullable = false)
    private VoteType voteType;

    @Column(name = "cast_at", nullable = false)
    private Instant castAt;
}

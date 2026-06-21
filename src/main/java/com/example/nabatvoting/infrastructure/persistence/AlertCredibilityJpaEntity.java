package com.example.nabatvoting.infrastructure.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "alert_credibility")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class AlertCredibilityJpaEntity {

    @Id
    @Column(name = "alert_id")
    private String alertId;

    @Column(name = "upvotes", nullable = false)
    private int upvotes;

    @Column(name = "downvotes", nullable = false)
    private int downvotes;

    @Column(name = "confirmations", nullable = false)
    private int confirmations;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}

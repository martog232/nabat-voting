package com.example.nabatvoting.application.projection;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Read-model projection that tracks the credibility score per alert.
 *
 * <p>The score is the net sum of votes: {@code +1} for each positive vote and
 * {@code -1} for each negative vote.  The projection is updated by the Kafka
 * consumer whenever a {@link VoteCastEvent} is received on the
 * {@value com.example.nabatvoting.infrastructure.kafka.KafkaTopics#VOTE_CAST}
 * topic.
 *
 * <p>In this reference implementation the projection is held in memory.  A
 * production deployment would typically persist this in a database or a
 * dedicated read-store.
 */
@Component
public class CredibilityProjection {

    private final Map<String, Integer> scoresByAlertId = new ConcurrentHashMap<>();

    /**
     * Applies a {@link VoteCastEvent} to the projection.
     *
     * @param event the event to apply
     */
    public void apply(VoteCastEvent event) {
        int delta = event.positive() ? 1 : -1;
        scoresByAlertId.merge(event.alertId(), delta, Integer::sum);
    }

    /**
     * Returns the current credibility score for the given alert, or {@code 0}
     * if no votes have been recorded yet.
     *
     * @param alertId the alert identifier
     * @return the current credibility score
     */
    public int getScore(String alertId) {
        return scoresByAlertId.getOrDefault(alertId, 0);
    }
}

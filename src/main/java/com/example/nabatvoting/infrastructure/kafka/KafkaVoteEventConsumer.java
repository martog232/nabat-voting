package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.port.in.MaintainCredibilityProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka listener that keeps the credibility read-model in sync.
 *
 * <p>One listener for casts and retractions alike: both arrive on {@link
 * KafkaTopics#VOTE_CHANGED}, and the projection recomputes the affected alert from the write
 * model rather than applying what the message carries, so the two need no separate handling
 * here. {@code changeType} is on the message for consumers that do care.
 */
@Component
public class KafkaVoteEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaVoteEventConsumer.class);

    private final MaintainCredibilityProjection credibilityProjection;

    public KafkaVoteEventConsumer(MaintainCredibilityProjection credibilityProjection) {
        this.credibilityProjection = credibilityProjection;
    }

    @KafkaListener(topics = KafkaTopics.VOTE_CHANGED, groupId = "${spring.kafka.consumer.group-id}")
    public void onVoteChanged(VoteChangedMessage message) {
        log.debug("Received {} for alert '{}'", message.changeType(), message.alertId());
        credibilityProjection.onVotesChanged(new AlertId(message.alertId()));
    }
}

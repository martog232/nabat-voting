package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.application.projection.CredibilityProjection;
import com.example.nabatvoting.domain.event.VoteCastEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka listener that updates the {@link CredibilityProjection} every time a
 * {@link VoteCastEvent} arrives on the {@value KafkaTopics#VOTE_CAST} topic.
 */
@Component
public class KafkaVoteEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaVoteEventConsumer.class);

    private final CredibilityProjection credibilityProjection;

    public KafkaVoteEventConsumer(CredibilityProjection credibilityProjection) {
        this.credibilityProjection = credibilityProjection;
    }

    @KafkaListener(topics = KafkaTopics.VOTE_CAST, groupId = "${spring.kafka.consumer.group-id}")
    public void onVoteCast(VoteCastEvent event) {
        log.debug("Received VoteCastEvent for alert '{}'", event.alertId());
        credibilityProjection.apply(event);
    }
}

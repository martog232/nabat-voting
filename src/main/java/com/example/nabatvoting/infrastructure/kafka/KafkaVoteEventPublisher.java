package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka-backed implementation of {@link VoteEventPublisher}.
 *
 * <p>Serialises {@link VoteCastEvent}s to JSON using the Jackson-based
 * {@link org.springframework.kafka.support.serializer.JsonSerializer} configured
 * in {@link com.example.nabatvoting.infrastructure.config.KafkaConfig} and
 * publishes them to the {@value KafkaTopics#VOTE_CAST} topic.
 */
@Component
public class KafkaVoteEventPublisher implements VoteEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaVoteEventPublisher.class);

    private final KafkaTemplate<String, VoteCastEvent> kafkaTemplate;

    public KafkaVoteEventPublisher(KafkaTemplate<String, VoteCastEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(VoteCastEvent event) {
        log.debug("Publishing VoteCastEvent for alert '{}' to topic '{}'",
                event.alertId(), KafkaTopics.VOTE_CAST);
        kafkaTemplate.send(KafkaTopics.VOTE_CAST, event.alertId(), event);
    }
}

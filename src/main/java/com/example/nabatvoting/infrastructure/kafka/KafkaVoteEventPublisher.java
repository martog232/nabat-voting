package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka-backed implementation of {@link VoteEventPublisher}.
 *
 * <p>Serialises events to JSON using the Jackson-based
 * {@link org.springframework.kafka.support.serializer.JsonSerializer} configured
 * in {@link com.example.nabatvoting.infrastructure.config.KafkaConfig} and
 * publishes them to the {@value KafkaTopics#VOTE_CAST} / {@value KafkaTopics#VOTE_REMOVED}
 * topics. The alert id is used as the partition key so that all events for an
 * alert preserve their relative order.
 */
@Component
public class KafkaVoteEventPublisher implements VoteEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaVoteEventPublisher.class);

    private final KafkaTemplate<String, VoteCastEvent> voteCastKafkaTemplate;
    private final KafkaTemplate<String, VoteRemovedEvent> voteRemovedKafkaTemplate;

    public KafkaVoteEventPublisher(KafkaTemplate<String, VoteCastEvent> voteCastKafkaTemplate,
                                   KafkaTemplate<String, VoteRemovedEvent> voteRemovedKafkaTemplate) {
        this.voteCastKafkaTemplate = voteCastKafkaTemplate;
        this.voteRemovedKafkaTemplate = voteRemovedKafkaTemplate;
    }

    @Override
    public void publish(VoteCastEvent event) {
        log.debug("Publishing VoteCastEvent for alert '{}' to topic '{}'",
                event.alertId(), KafkaTopics.VOTE_CAST);
        voteCastKafkaTemplate.send(KafkaTopics.VOTE_CAST, event.alertId(), event);
    }

    @Override
    public void publishRemoved(VoteRemovedEvent event) {
        log.debug("Publishing VoteRemovedEvent for alert '{}' to topic '{}'",
                event.alertId(), KafkaTopics.VOTE_REMOVED);
        voteRemovedKafkaTemplate.send(KafkaTopics.VOTE_REMOVED, event.alertId(), event);
    }
}

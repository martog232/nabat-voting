package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.port.in.MaintainCredibilityProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka listeners that keep the credibility read-model in sync. Both vote casts
 * and vote removals drive the {@link MaintainCredibilityProjection} inbound port,
 * which recomputes the affected alert's projection from the write-model.
 */
@Component
public class KafkaVoteEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaVoteEventConsumer.class);

    private final MaintainCredibilityProjection credibilityProjection;

    public KafkaVoteEventConsumer(MaintainCredibilityProjection credibilityProjection) {
        this.credibilityProjection = credibilityProjection;
    }

    @KafkaListener(topics = KafkaTopics.VOTE_CAST, groupId = "${spring.kafka.consumer.group-id}")
    public void onVoteCast(VoteCastEvent event) {
        log.debug("Received VoteCastEvent for alert '{}'", event.alertId());
        credibilityProjection.onVoteCast(event);
    }

    @KafkaListener(
            topics = KafkaTopics.VOTE_REMOVED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "voteRemovedKafkaListenerContainerFactory"
    )
    public void onVoteRemoved(VoteRemovedEvent event) {
        log.debug("Received VoteRemovedEvent for alert '{}'", event.alertId());
        credibilityProjection.onVoteRemoved(event);
    }
}

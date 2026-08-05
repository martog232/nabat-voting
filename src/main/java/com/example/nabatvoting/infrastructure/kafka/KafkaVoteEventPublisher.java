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
 * <p>Events are keyed by alert id, so all events for one alert land on the same
 * partition and keep their relative order.
 *
 * <h2>Send failures are observed, not ignored</h2>
 * {@code KafkaTemplate.send()} is asynchronous and the returned future used to be
 * discarded. A broker outage therefore failed silently: the vote was committed, no
 * event was ever published, and the credibility projection stayed stale forever with
 * nothing in the logs to say why. The callbacks below at least make that loud.
 *
 * <p><strong>Known limitation:</strong> this is still a dual write. The database
 * transaction and the Kafka publish are not atomic, so a crash between commit and
 * send loses the event (and a rollback after a successful send publishes an event for
 * a vote that does not exist). A transactional outbox is the correct fix; the
 * projection being rebuildable from the write model via
 * {@code POST /api/v1/admin/credibility/rebuild} is the current mitigation.
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

        voteCastKafkaTemplate.send(KafkaTopics.VOTE_CAST, event.alertId(), event)
                .whenComplete((result, failure) -> logOutcome(KafkaTopics.VOTE_CAST, event.alertId(), failure));
    }

    @Override
    public void publishRemoved(VoteRemovedEvent event) {
        log.debug("Publishing VoteRemovedEvent for alert '{}' to topic '{}'",
                event.alertId(), KafkaTopics.VOTE_REMOVED);

        voteRemovedKafkaTemplate.send(KafkaTopics.VOTE_REMOVED, event.alertId(), event)
                .whenComplete((result, failure) -> logOutcome(KafkaTopics.VOTE_REMOVED, event.alertId(), failure));
    }

    private static void logOutcome(String topic, String alertId, Throwable failure) {
        if (failure != null) {
            log.error("Failed to publish to {} for alert '{}'. The credibility projection for this "
                            + "alert will remain stale until the next vote or an admin rebuild.",
                    topic, alertId, failure);
        }
    }
}

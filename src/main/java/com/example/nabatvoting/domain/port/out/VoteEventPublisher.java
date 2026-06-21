package com.example.nabatvoting.domain.port.out;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;

/**
 * Outbound port: publishes domain events about votes.
 *
 * <p>The primary implementation uses Kafka.
 */
public interface VoteEventPublisher {

    /**
     * Publishes a {@link VoteCastEvent} so that downstream consumers can react.
     *
     * @param event the event to publish
     */
    void publish(VoteCastEvent event);

    /**
     * Publishes a {@link VoteRemovedEvent} when a voter retracts their vote.
     *
     * @param event the event to publish
     */
    void publishRemoved(VoteRemovedEvent event);
}

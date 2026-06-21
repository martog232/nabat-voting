package com.example.nabatvoting.domain.port.in;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;

/**
 * Inbound port driven by the event stream: keeps the credibility read-model in
 * sync as vote events arrive. Implemented by the application layer and invoked
 * by the Kafka consumer adapter.
 */
public interface MaintainCredibilityProjection {

    /**
     * Applies a vote-cast event to the projection. Implementations must be
     * idempotent so that at-least-once delivery (a redelivered event) does not
     * corrupt the counts.
     */
    void onVoteCast(VoteCastEvent event);

    /**
     * Applies a vote-removed event to the projection. Idempotent, like
     * {@link #onVoteCast}.
     */
    void onVoteRemoved(VoteRemovedEvent event);
}

package com.example.nabatvoting.domain.port.in;

import com.example.nabatvoting.domain.model.AlertId;

/**
 * Inbound port driven by the event stream: keeps the credibility read-model in sync as vote
 * events arrive. Implemented by the application layer and invoked by the Kafka consumer
 * adapter.
 *
 * <p>One method, taking only the alert, because that is all the projection ever used. It had
 * two — {@code onVoteCast} and {@code onVoteRemoved} — whose bodies were the same call, and
 * they only looked different because the events did. The projection recomputes from the
 * write model, so what changed does not matter, only which alert changed.
 *
 * <p>Implementations must be idempotent: at-least-once delivery means the same change will
 * sometimes arrive twice. Recomputing satisfies that for free.
 */
public interface MaintainCredibilityProjection {

    void onVotesChanged(AlertId alertId);
}

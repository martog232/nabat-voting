package com.example.nabatvoting.domain.port.out;

import com.example.nabatvoting.domain.model.AlertId;

/**
 * Serialises the votes cast on one alert against each other.
 *
 * <p>{@code CastVoteService} reads the tallies back from the write model and puts those
 * <em>absolute</em> numbers on the event it publishes. That read and the write before it have
 * to be atomic with respect to another vote on the same alert, or two concurrent voters each
 * read a total that excludes the other and both publish the same stale count — leaving the
 * consumer's projection permanently short. See {@code V5__create_alert_vote_lock_table.sql}
 * for the full account.
 *
 * <p>Held for the duration of the calling transaction and released when it ends, whichever
 * way it ends. Contends only with another vote on the same alert.
 */
public interface AlertVoteLock {

    /**
     * Blocks until no other transaction is voting on this alert, then holds the lock until
     * the caller's transaction commits or rolls back.
     *
     * <p>Must be called inside a transaction, and before the tallies are read.
     */
    void acquire(AlertId alertId);
}

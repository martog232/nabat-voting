package com.example.nabatvoting.infrastructure.kafka;

/**
 * Central registry of Kafka topic names used by the voting module.
 */
public final class KafkaTopics {

    /** Topic on which {@link com.example.nabatvoting.domain.event.VoteCastEvent}s are published. */
    public static final String VOTE_CAST = "vote.cast";

    /** Topic on which {@link com.example.nabatvoting.domain.event.VoteRemovedEvent}s are published. */
    public static final String VOTE_REMOVED = "vote.removed";

    private KafkaTopics() {
    }
}

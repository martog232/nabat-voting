package com.example.nabatvoting.infrastructure.kafka;

/**
 * Central registry of Kafka topic names used by the voting module.
 */
public final class KafkaTopics {

    /**
     * Every change to an alert's votes, cast and retraction alike, keyed by alert id.
     *
     * <p>One topic rather than two on purpose. Kafka orders messages within a partition, and
     * a key picks the partition — so one topic keyed by alert means everything said about one
     * alert arrives in the order it happened. Two topics have two sets of partitions and two
     * consumer containers polling them independently, so a vote and its immediate retraction
     * could be applied in either order. A consumer that recomputes from the write model does
     * not care; one that applies the counts it is told does, and it then holds the wrong
     * numbers until the next vote on that alert.
     *
     * <p>The alternative was a watermark on the consumer's side — carry the event time, keep
     * the last applied one, ignore anything older. It compensates for the disorder rather
     * than preventing it, needs every consumer to implement it, and is only as good as the
     * producer's clock.
     */
    public static final String VOTE_CHANGED = "vote.changed";

    private KafkaTopics() {
    }
}

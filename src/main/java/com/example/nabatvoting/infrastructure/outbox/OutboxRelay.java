package com.example.nabatvoting.infrastructure.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Sends committed outbox rows to Kafka and marks them published.
 *
 * <p>Delivery is at-least-once, which is the strongest thing an outbox can offer: the send
 * and the {@code published_at} update cannot be atomic either, so a crash between them
 * re-sends. That is safe here because the consumer recomputes each alert's projection from
 * the write model, so a replayed event produces the same row.
 *
 * <p><b>A failed send stops its batch.</b> Rows are keyed by alert and ordered by age, so
 * skipping past a failure to reach a later row could put two events for the same alert on
 * the topic out of order. The projection would survive it — recomputation does not care
 * about order — but the topic is a public contract and consumers that do care will exist.
 * Stopping costs one poll interval; nothing is dropped.
 *
 * <p><b>Startup needs no special case.</b> Rows left pending by a crash are simply the
 * oldest ones, so the first poll picks them up.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventJpaRepository outbox;
    private final KafkaTemplate<String, byte[]> outboxKafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;
    private final Duration sendTimeout;
    private final Duration retention;

    public OutboxRelay(OutboxEventJpaRepository outbox,
                       KafkaTemplate<String, byte[]> outboxKafkaTemplate,
                       TransactionTemplate transactionTemplate,
                       @Value("${nabat.outbox.batch-size:100}") int batchSize,
                       @Value("${nabat.outbox.send-timeout:PT10S}") Duration sendTimeout,
                       @Value("${nabat.outbox.retention:P7D}") Duration retention) {
        this.outbox = outbox;
        this.outboxKafkaTemplate = outboxKafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.batchSize = batchSize;
        this.sendTimeout = sendTimeout;
        this.retention = retention;
    }

    /**
     * Drains the backlog, a batch at a time.
     *
     * <p>{@code fixedDelay}, not {@code fixedRate}: a slow batch must not have the next one
     * start on top of it. The loop is what keeps a backlog from taking one poll interval per
     * batch to clear — a partial batch means there is nothing left to claim.
     */
    @Scheduled(fixedDelayString = "${nabat.outbox.poll-interval:PT1S}")
    public void publishPending() {
        while (publishBatch() == batchSize) {
            // Full batch: there may be more waiting.
        }
    }

    private int publishBatch() {
        Integer published = transactionTemplate.execute(status -> {
            List<OutboxEventJpaEntity> pending = outbox.lockPending(PageRequest.of(0, batchSize));
            int sent = 0;

            for (OutboxEventJpaEntity event : pending) {
                if (!send(event)) {
                    break;
                }
                sent++;
            }
            return sent;
        });

        return published == null ? 0 : published;
    }

    private boolean send(OutboxEventJpaEntity event) {
        try {
            outboxKafkaTemplate
                    .send(event.getTopic(), event.getPartitionKey(), event.getPayload())
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
            event.markPublished(Instant.now());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            event.markFailed(e);
            return false;
        } catch (Exception e) {
            event.markFailed(e);
            log.error("Outbox send failed for {} on '{}' (attempt {}); it stays pending and the "
                            + "rest of this batch waits behind it.",
                    event.getEventType(), event.getTopic(), event.getAttempts(), e);
            return false;
        }
    }

    /**
     * Drops rows that have been published for longer than the retention window.
     *
     * <p>Published rows are of no use to the relay, but they are the only record of what was
     * sent when, so they are kept for a while rather than deleted on publish. Without this the
     * table only ever grows, and its partial index with it.
     */
    @Scheduled(fixedDelayString = "${nabat.outbox.purge-interval:PT1H}",
               initialDelayString = "${nabat.outbox.purge-interval:PT1H}")
    public void purgePublished() {
        Instant before = Instant.now().minus(retention);
        Integer deleted = transactionTemplate.execute(status -> outbox.deletePublishedBefore(before));

        if (deleted != null && deleted > 0) {
            log.info("Purged {} outbox row(s) published before {}", deleted, before);
        }
    }
}

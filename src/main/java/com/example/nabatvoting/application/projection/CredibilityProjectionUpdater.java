package com.example.nabatvoting.application.projection;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.model.AlertCredibility;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteCounts;
import com.example.nabatvoting.domain.port.in.MaintainCredibilityProjection;
import com.example.nabatvoting.domain.port.in.RebuildCredibilityProjectionUseCase;
import com.example.nabatvoting.domain.port.out.CredibilityProjectionStore;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

/**
 * Maintains the {@link AlertCredibility} read-model.
 *
 * <p>Rather than applying {@code +1 / -1} deltas (which would double-count under
 * Kafka's at-least-once delivery and could not handle vote removals), this
 * projection <em>recomputes</em> the affected alert's counts from the
 * {@code votes} write-model on every event. Recomputation is naturally
 * idempotent: replaying the same event produces the same row, so the projection
 * is self-healing.
 *
 * <p>The trade-off is eventual consistency — there is a small window between a
 * vote being committed to {@code votes} and the projection catching up after the
 * Kafka round-trip. Callers that need read-your-writes must use the stats
 * returned directly by {@code CastVoteUseCase.castVote}/{@code removeVote},
 * which read the write model inside the mutating transaction.
 */
@Service
public class CredibilityProjectionUpdater
        implements MaintainCredibilityProjection, RebuildCredibilityProjectionUseCase {

    private static final Logger log = LoggerFactory.getLogger(CredibilityProjectionUpdater.class);

    private final VoteRepository voteRepository;
    private final CredibilityProjectionStore projectionStore;
    private final TransactionTemplate transactionTemplate;

    public CredibilityProjectionUpdater(VoteRepository voteRepository,
                                        CredibilityProjectionStore projectionStore,
                                        TransactionTemplate transactionTemplate) {
        this.voteRepository = voteRepository;
        this.projectionStore = projectionStore;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    @Transactional
    public void onVoteCast(VoteCastEvent event) {
        recompute(new AlertId(event.alertId()));
    }

    @Override
    @Transactional
    public void onVoteRemoved(VoteRemovedEvent event) {
        recompute(new AlertId(event.alertId()));
    }

    /**
     * Replays the whole projection, one transaction per alert.
     *
     * <p>Deliberately not a single {@code @Transactional} method: wrapping every
     * alert in one transaction meant an unbounded write transaction that would
     * hold locks and eventually time out on any realistic data volume. Per-alert
     * transactions make the rebuild restartable and keep each one small — a
     * partial rebuild leaves correct rows behind, since each recompute is
     * independent and idempotent.
     */
    @Override
    public int rebuildAll() {
        List<AlertId> alertIds = transactionTemplate.execute(status -> voteRepository.findDistinctAlertIds());
        if (alertIds == null || alertIds.isEmpty()) {
            log.info("Credibility projection rebuild: no alerts with votes");
            return 0;
        }

        int rebuilt = 0;
        for (AlertId alertId : alertIds) {
            try {
                transactionTemplate.executeWithoutResult(status -> recompute(alertId));
                rebuilt++;
            } catch (RuntimeException ex) {
                // Keep going: one bad alert must not abort the whole replay.
                log.warn("Credibility projection rebuild failed for alert '{}': {}",
                        alertId.value(), ex.getMessage());
            }
        }

        log.info("Rebuilt credibility projection for {}/{} alert(s)", rebuilt, alertIds.size());
        return rebuilt;
    }

    private void recompute(AlertId alertId) {
        VoteCounts counts = voteRepository.countsFor(alertId);
        projectionStore.save(AlertCredibility.of(alertId, counts));
        log.debug("Recomputed credibility projection for alert '{}': up={}, down={}, confirm={}, score={}",
                alertId.value(), counts.upvotes(), counts.downvotes(), counts.confirmations(),
                counts.credibilityScore());
    }
}

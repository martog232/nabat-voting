package com.example.nabatvoting.application.projection;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.model.AlertCredibility;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.port.in.MaintainCredibilityProjection;
import com.example.nabatvoting.domain.port.in.RebuildCredibilityProjectionUseCase;
import com.example.nabatvoting.domain.port.out.CredibilityProjectionStore;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
 * Kafka round-trip. That window is the read-model lag inherent to CQRS.
 */
@Service
public class CredibilityProjectionUpdater
        implements MaintainCredibilityProjection, RebuildCredibilityProjectionUseCase {

    private static final Logger log = LoggerFactory.getLogger(CredibilityProjectionUpdater.class);

    private final VoteRepository voteRepository;
    private final CredibilityProjectionStore projectionStore;

    public CredibilityProjectionUpdater(VoteRepository voteRepository,
                                        CredibilityProjectionStore projectionStore) {
        this.voteRepository = voteRepository;
        this.projectionStore = projectionStore;
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

    @Override
    @Transactional
    public int rebuildAll() {
        List<AlertId> alertIds = voteRepository.findDistinctAlertIds();
        alertIds.forEach(this::recompute);
        log.info("Rebuilt credibility projection for {} alert(s)", alertIds.size());
        return alertIds.size();
    }

    private void recompute(AlertId alertId) {
        int upvotes = voteRepository.countUpvotes(alertId);
        int downvotes = voteRepository.countDownvotes(alertId);
        int confirmations = voteRepository.countConfirmations(alertId);

        projectionStore.save(new AlertCredibility(alertId, upvotes, downvotes, confirmations));
        log.debug("Recomputed credibility projection for alert '{}': up={}, down={}, confirm={}",
                alertId.value(), upvotes, downvotes, confirmations);
    }
}

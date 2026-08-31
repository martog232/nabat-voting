package com.example.nabatvoting.application.service;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.event.VoteTallies;
import com.example.nabatvoting.domain.exception.DuplicateVoteException;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoteCounts;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoteType;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.in.CastVoteUseCase;
import com.example.nabatvoting.domain.port.out.CredibilityProjectionStore;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Service
public class CastVoteService implements CastVoteUseCase {

    private final VoteRepository voteRepository;
    private final VoteEventPublisher voteEventPublisher;
    private final CredibilityProjectionStore credibilityProjectionStore;

    public CastVoteService(VoteRepository voteRepository,
                           VoteEventPublisher voteEventPublisher,
                           CredibilityProjectionStore credibilityProjectionStore) {
        this.voteRepository = voteRepository;
        this.voteEventPublisher = voteEventPublisher;
        this.credibilityProjectionStore = credibilityProjectionStore;
    }

    @Override
    @Transactional
    public CastVoteResult castVote(CastVoteCommand command) {
        Instant now = Instant.now();
        Optional<Vote> existing = voteRepository.findByAlertIdAndVoterId(command.alertId(), command.voterId());

        VoteId voteId;
        boolean created;

        if (existing.isPresent()) {
            Vote current = existing.get();
            if (current.getVoteType() == command.voteType()) {
                // Re-casting the identical vote is a no-op conflict, not a 500.
                throw new DuplicateVoteException(
                        "Voter has already cast a " + command.voteType() + " vote on this alert");
            }
            // Change of mind: overwrite the existing vote row (same id) in place.
            Vote changed = new Vote(current.getId(), command.alertId(), command.voterId(),
                    command.voteType(), now);
            voteRepository.save(changed);
            voteId = current.getId();
            created = false;
        } else {
            voteId = VoteId.generate();
            Vote vote = new Vote(voteId, command.alertId(), command.voterId(), command.voteType(), now);
            // A concurrent first vote by the same voter loses the race on the
            // (alert_id, voter_id) unique constraint; that surfaces as a 409 too.
            voteRepository.save(vote);
            created = true;
        }

        // Read once, after the write, and used twice: the caller's response and the event
        // both need the tallies as of this vote, and they must not be two different reads.
        VoteCounts counts = countsFromWriteModel(command.alertId());

        voteEventPublisher.publish(VoteCastEvent.of(
                voteId.value(),
                command.alertId().value(),
                command.voterId().value(),
                command.voteType(),
                now,
                VoteTallies.from(counts)
        ));

        return new CastVoteResult(voteId, created, now, VoteStats.from(counts));
    }

    @Override
    @Transactional
    public VoteStats removeVote(AlertId alertId, VoterId voterId) {
        voteRepository.deleteByAlertIdAndVoterId(alertId, voterId);

        VoteCounts counts = countsFromWriteModel(alertId);

        // Emit a removal event so the read-models catch up. Idempotent on the consumer
        // side, so emitting even when nothing was deleted is harmless.
        voteEventPublisher.publishRemoved(VoteRemovedEvent.of(
                alertId.value(), voterId.value(), Instant.now(), VoteTallies.from(counts)));

        return VoteStats.from(counts);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<VoteType> getUserVote(AlertId alertId, VoterId voterId) {
        return voteRepository.findByAlertIdAndVoterId(alertId, voterId)
                .map(Vote::getVoteType);
    }

    @Override
    @Transactional(readOnly = true)
    public VoteStats getVoteStats(AlertId alertId) {
        return credibilityProjectionStore.findByAlertId(alertId)
                .map(c -> VoteStats.from(c.counts()))
                .orElseGet(() -> VoteStats.from(VoteCounts.EMPTY));
    }

    /**
     * Tallies read back from the write model within the mutating transaction.
     *
     * <p>Deliberately <em>not</em> read from the {@code alert_credibility}
     * projection: that is updated asynchronously off the Kafka topic, so at this
     * point it still holds the pre-vote counts. Returning it here is what made a
     * caller's own vote appear not to register until somebody else voted.
     *
     * <p>The same value goes onto the event, which is what lets a consumer elsewhere hold a
     * projection without ever reading this service's own.
     */
    private VoteCounts countsFromWriteModel(AlertId alertId) {
        return voteRepository.countsFor(alertId);
    }
}

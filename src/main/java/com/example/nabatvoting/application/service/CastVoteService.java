package com.example.nabatvoting.application.service;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.in.CastVoteUseCase;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class CastVoteService implements CastVoteUseCase {

    private final VoteRepository voteRepository;
    private final VoteEventPublisher voteEventPublisher;

    public CastVoteService(VoteRepository voteRepository, VoteEventPublisher voteEventPublisher) {
        this.voteRepository = voteRepository;
        this.voteEventPublisher = voteEventPublisher;
    }

    @Override
    @Transactional
    public VoteId castVote(CastVoteCommand command) {
        VoteId voteId = VoteId.generate();
        Instant now = Instant.now();

        Vote vote = new Vote(voteId, command.alertId(), command.voterId(), command.voteType(), now);
        voteRepository.save(vote);

        VoteCastEvent event = VoteCastEvent.of(
                voteId.value(),
                command.alertId().value(),
                command.voterId().value(),
                command.voteType(),
                now
        );
        voteEventPublisher.publish(event);

        return voteId;
    }

    @Override
    @Transactional
    public void removeVote(AlertId alertId, VoterId voterId) {
        voteRepository.deleteByAlertIdAndVoterId(alertId, voterId);
    }

    @Override
    @Transactional(readOnly = true)
    public boolean hasUserVoted(AlertId alertId, VoterId voterId) {
        return voteRepository.findByAlertIdAndVoterId(alertId, voterId).isPresent();
    }

    @Override
    @Transactional(readOnly = true)
    public VoteStats getVoteStats(AlertId alertId) {
        int upvotes = voteRepository.countUpvotes(alertId);
        int downvotes = voteRepository.countDownvotes(alertId);
        int confirmations = voteRepository.countConfirmations(alertId);
        return VoteStats.fromCounts(upvotes, downvotes, confirmations);
    }
}

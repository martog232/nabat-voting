package com.example.nabatvoting.application.service;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.in.CastVoteUseCase;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Application service that implements the {@link CastVoteUseCase}.
 *
 * <p>Orchestrates persisting the vote and publishing a {@link VoteCastEvent}
 * to Kafka so that downstream services (e.g. the credibility projection) can
 * react asynchronously.
 */
@Service
public class CastVoteService implements CastVoteUseCase {

    private final VoteRepository voteRepository;
    private final VoteEventPublisher voteEventPublisher;

    public CastVoteService(VoteRepository voteRepository, VoteEventPublisher voteEventPublisher) {
        this.voteRepository = voteRepository;
        this.voteEventPublisher = voteEventPublisher;
    }

    @Override
    public VoteId castVote(CastVoteCommand command) {
        VoteId voteId = VoteId.generate();
        Instant now = Instant.now();

        Vote vote = new Vote(voteId, command.alertId(), command.voterId(), command.positive(), now);
        voteRepository.save(vote);

        VoteCastEvent event = VoteCastEvent.of(
                voteId.value(),
                command.alertId().value(),
                command.voterId().value(),
                command.positive(),
                now
        );
        voteEventPublisher.publish(event);

        return voteId;
    }
}

package com.example.nabatvoting.application.service;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class CastVoteServiceTest {

    private VoteRepository voteRepository;
    private VoteEventPublisher voteEventPublisher;
    private CastVoteService service;

    @BeforeEach
    void setUp() {
        voteRepository = mock(VoteRepository.class);
        voteEventPublisher = mock(VoteEventPublisher.class);
        service = new CastVoteService(voteRepository, voteEventPublisher);
    }

    @Test
    void castVote_savesVoteAndPublishesEvent() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-1"), new VoterId("voter-A"), true);

        VoteId returnedId = service.castVote(command);

        // verify vote is persisted
        ArgumentCaptor<Vote> voteCaptor = ArgumentCaptor.forClass(Vote.class);
        verify(voteRepository).save(voteCaptor.capture());
        Vote savedVote = voteCaptor.getValue();
        assertThat(savedVote.getAlertId().value()).isEqualTo("alert-1");
        assertThat(savedVote.getVoterId().value()).isEqualTo("voter-A");
        assertThat(savedVote.isPositive()).isTrue();
        assertThat(savedVote.getCastAt()).isNotNull();

        // verify event is published
        ArgumentCaptor<VoteCastEvent> eventCaptor = ArgumentCaptor.forClass(VoteCastEvent.class);
        verify(voteEventPublisher).publish(eventCaptor.capture());
        VoteCastEvent publishedEvent = eventCaptor.getValue();
        assertThat(publishedEvent.alertId()).isEqualTo("alert-1");
        assertThat(publishedEvent.voterId()).isEqualTo("voter-A");
        assertThat(publishedEvent.positive()).isTrue();
        assertThat(publishedEvent.voteId()).isEqualTo(returnedId.value());
    }

    @Test
    void castVote_negativeVote_publishesCorrectEvent() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-2"), new VoterId("voter-B"), false);

        service.castVote(command);

        ArgumentCaptor<VoteCastEvent> eventCaptor = ArgumentCaptor.forClass(VoteCastEvent.class);
        verify(voteEventPublisher).publish(eventCaptor.capture());
        assertThat(eventCaptor.getValue().positive()).isFalse();
    }

    @Test
    void castVote_returnsGeneratedVoteId() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-3"), new VoterId("voter-C"), true);

        VoteId id = service.castVote(command);

        assertThat(id).isNotNull();
        assertThat(id.value()).isNotNull();
    }
}

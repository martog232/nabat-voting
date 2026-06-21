package com.example.nabatvoting.application.service;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.exception.DuplicateVoteException;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.Vote;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoteType;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.model.AlertCredibility;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.in.CastVoteUseCase;
import com.example.nabatvoting.domain.port.out.CredibilityProjectionStore;
import com.example.nabatvoting.domain.port.out.VoteEventPublisher;
import com.example.nabatvoting.domain.port.out.VoteRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class CastVoteServiceTest {

    private VoteRepository voteRepository;
    private VoteEventPublisher voteEventPublisher;
    private CredibilityProjectionStore credibilityProjectionStore;
    private CastVoteService service;

    @BeforeEach
    void setUp() {
        voteRepository = mock(VoteRepository.class);
        voteEventPublisher = mock(VoteEventPublisher.class);
        credibilityProjectionStore = mock(CredibilityProjectionStore.class);
        service = new CastVoteService(voteRepository, voteEventPublisher, credibilityProjectionStore);
    }

    @Test
    void getVoteStats_readsFromProjection_notFromWriteModel() {
        AlertId alertId = new AlertId("alert-stats");
        when(credibilityProjectionStore.findByAlertId(alertId))
                .thenReturn(Optional.of(new AlertCredibility(alertId, 3, 1, 2)));

        CastVoteUseCase.VoteStats stats = service.getVoteStats(alertId);

        assertThat(stats.upvotes()).isEqualTo(3);
        assertThat(stats.downvotes()).isEqualTo(1);
        assertThat(stats.confirmations()).isEqualTo(2);
        // 3 - 1 + (2 * 2)
        assertThat(stats.credibilityScore()).isEqualTo(6);
        // Stats must come from the read-model, never by aggregating votes.
        verify(voteRepository, never()).countUpvotes(any());
    }

    @Test
    void getVoteStats_returnsZeroes_whenNoProjectionYet() {
        AlertId alertId = new AlertId("alert-empty");
        when(credibilityProjectionStore.findByAlertId(alertId)).thenReturn(Optional.empty());

        CastVoteUseCase.VoteStats stats = service.getVoteStats(alertId);

        assertThat(stats.upvotes()).isZero();
        assertThat(stats.credibilityScore()).isZero();
    }

    @Test
    void castVote_savesVoteAndPublishesEvent() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-1"), new VoterId("voter-A"), VoteType.UPVOTE);

        CastVoteUseCase.CastVoteResult result = service.castVote(command);
        VoteId returnedId = result.voteId();
        assertThat(result.created()).isTrue();

        ArgumentCaptor<Vote> voteCaptor = ArgumentCaptor.forClass(Vote.class);
        verify(voteRepository).save(voteCaptor.capture());
        Vote savedVote = voteCaptor.getValue();
        assertThat(savedVote.getAlertId().value()).isEqualTo("alert-1");
        assertThat(savedVote.getVoterId().value()).isEqualTo("voter-A");
        assertThat(savedVote.getVoteType()).isEqualTo(VoteType.UPVOTE);
        assertThat(savedVote.getCastAt()).isNotNull();

        ArgumentCaptor<VoteCastEvent> eventCaptor = ArgumentCaptor.forClass(VoteCastEvent.class);
        verify(voteEventPublisher).publish(eventCaptor.capture());
        VoteCastEvent publishedEvent = eventCaptor.getValue();
        assertThat(publishedEvent.alertId()).isEqualTo("alert-1");
        assertThat(publishedEvent.voterId()).isEqualTo("voter-A");
        assertThat(publishedEvent.voteType()).isEqualTo(VoteType.UPVOTE);
        assertThat(publishedEvent.voteId()).isEqualTo(returnedId.value());
    }

    @Test
    void castVote_downvote_publishesCorrectEvent() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-2"), new VoterId("voter-B"), VoteType.DOWNVOTE);

        service.castVote(command);

        ArgumentCaptor<VoteCastEvent> eventCaptor = ArgumentCaptor.forClass(VoteCastEvent.class);
        verify(voteEventPublisher).publish(eventCaptor.capture());
        assertThat(eventCaptor.getValue().voteType()).isEqualTo(VoteType.DOWNVOTE);
    }

    @Test
    void castVote_returnsGeneratedVoteId() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-3"), new VoterId("voter-C"), VoteType.CONFIRM);

        VoteId id = service.castVote(command).voteId();

        assertThat(id).isNotNull();
        assertThat(id.value()).isNotNull();
    }

    @Test
    void castVote_sameVoteTypeAgain_throwsDuplicateAndDoesNotSave() {
        AlertId alertId = new AlertId("alert-dup");
        VoterId voterId = new VoterId("voter-D");
        Vote existing = new Vote(VoteId.generate(), alertId, voterId, VoteType.UPVOTE, Instant.now());
        when(voteRepository.findByAlertIdAndVoterId(alertId, voterId)).thenReturn(Optional.of(existing));

        CastVoteCommand command = new CastVoteCommand(alertId, voterId, VoteType.UPVOTE);

        assertThatThrownBy(() -> service.castVote(command))
                .isInstanceOf(DuplicateVoteException.class);

        verify(voteRepository, never()).save(any());
        verify(voteEventPublisher, never()).publish(any());
    }

    @Test
    void castVote_differentVoteType_upsertsInPlaceAndReportsNotCreated() {
        AlertId alertId = new AlertId("alert-change");
        VoterId voterId = new VoterId("voter-E");
        VoteId existingId = VoteId.generate();
        Vote existing = new Vote(existingId, alertId, voterId, VoteType.UPVOTE, Instant.now());
        when(voteRepository.findByAlertIdAndVoterId(alertId, voterId)).thenReturn(Optional.of(existing));

        CastVoteCommand command = new CastVoteCommand(alertId, voterId, VoteType.DOWNVOTE);

        CastVoteUseCase.CastVoteResult result = service.castVote(command);

        assertThat(result.created()).isFalse();
        assertThat(result.voteId()).isEqualTo(existingId);

        ArgumentCaptor<Vote> voteCaptor = ArgumentCaptor.forClass(Vote.class);
        verify(voteRepository).save(voteCaptor.capture());
        Vote saved = voteCaptor.getValue();
        // Same row (same id), new type.
        assertThat(saved.getId()).isEqualTo(existingId);
        assertThat(saved.getVoteType()).isEqualTo(VoteType.DOWNVOTE);

        ArgumentCaptor<VoteCastEvent> eventCaptor = ArgumentCaptor.forClass(VoteCastEvent.class);
        verify(voteEventPublisher).publish(eventCaptor.capture());
        assertThat(eventCaptor.getValue().voteType()).isEqualTo(VoteType.DOWNVOTE);
    }

    @Test
    void getUserVote_returnsStoredVoteType() {
        AlertId alertId = new AlertId("alert-me");
        VoterId voterId = new VoterId("voter-G");
        Vote existing = new Vote(VoteId.generate(), alertId, voterId, VoteType.CONFIRM, Instant.now());
        when(voteRepository.findByAlertIdAndVoterId(alertId, voterId)).thenReturn(Optional.of(existing));

        assertThat(service.getUserVote(alertId, voterId)).contains(VoteType.CONFIRM);
    }

    @Test
    void getUserVote_returnsEmpty_whenNoVote() {
        AlertId alertId = new AlertId("alert-me-none");
        VoterId voterId = new VoterId("voter-H");
        when(voteRepository.findByAlertIdAndVoterId(alertId, voterId)).thenReturn(Optional.empty());

        assertThat(service.getUserVote(alertId, voterId)).isEmpty();
    }

    @Test
    void removeVote_deletesAndPublishesRemovedEvent() {
        AlertId alertId = new AlertId("alert-rm");
        VoterId voterId = new VoterId("voter-F");

        service.removeVote(alertId, voterId);

        verify(voteRepository).deleteByAlertIdAndVoterId(alertId, voterId);

        ArgumentCaptor<VoteRemovedEvent> eventCaptor = ArgumentCaptor.forClass(VoteRemovedEvent.class);
        verify(voteEventPublisher).publishRemoved(eventCaptor.capture());
        VoteRemovedEvent event = eventCaptor.getValue();
        assertThat(event.alertId()).isEqualTo("alert-rm");
        assertThat(event.voterId()).isEqualTo("voter-F");
        assertThat(event.removedAt()).isNotNull();
    }
}

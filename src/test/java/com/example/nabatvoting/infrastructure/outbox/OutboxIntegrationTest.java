package com.example.nabatvoting.infrastructure.outbox;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteType;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.in.CastVoteUseCase;
import com.example.nabatvoting.infrastructure.kafka.KafkaTopics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * What the outbox is for: the event and the vote it describes share one transaction.
 *
 * <p>The dual write this replaced could fail in two directions, and only one of them was
 * ever visible. A crash between commit and send dropped the event; a send that overtook its
 * own commit let the consumer recompute the projection from a write model without the vote
 * in it, storing zeros permanently. Neither could be written as a test, because both are
 * races — which is the point of the fix: after it, the ordering is a property of the
 * database, so it can be asserted rather than awaited.
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {KafkaTopics.VOTE_CAST, KafkaTopics.VOTE_REMOVED},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext
class OutboxIntegrationTest {

    @Autowired
    private CastVoteUseCase castVoteUseCase;

    @Autowired
    private OutboxEventJpaRepository outbox;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private JsonMapper mapper;

    private List<OutboxEventJpaEntity> rowsFor(String alertId) {
        return outbox.findAll().stream()
                .filter(row -> row.getPartitionKey().equals(alertId))
                .toList();
    }

    /**
     * The event is visible inside the writing transaction, and disappears with it.
     *
     * <p>Rolling back is how the two halves get asserted at once: a row that is there before
     * the commit and gone after it cannot have been sent to a broker in between.
     */
    @Test
    void theEventIsWrittenInTheVotesOwnTransaction_andRollsBackWithIt() {
        String alertId = "alert-outbox-rollback";
        VoterId voter = new VoterId("voter-outbox-rollback");

        List<OutboxEventJpaEntity> pendingBeforeRollback = transactionTemplate.execute(status -> {
            castVoteUseCase.castVote(new CastVoteCommand(new AlertId(alertId), voter, VoteType.UPVOTE));
            List<OutboxEventJpaEntity> rows = rowsFor(alertId);
            status.setRollbackOnly();
            return rows;
        });

        assertThat(pendingBeforeRollback).hasSize(1);
        OutboxEventJpaEntity row = pendingBeforeRollback.getFirst();
        assertThat(row.getTopic()).isEqualTo(KafkaTopics.VOTE_CAST);
        assertThat(row.getPublishedAt()).isNull();

        // The stored bytes are the bytes that go on the wire, so a payload that cannot be
        // read back is a permanently undeliverable row. Reading it back is cheap insurance.
        VoteCastEvent event = mapper.readValue(row.getPayload(), VoteCastEvent.class);
        assertThat(event.alertId()).isEqualTo(alertId);
        assertThat(event.voterId()).isEqualTo(voter.value());
        assertThat(event.voteType()).isEqualTo(VoteType.UPVOTE);
        assertThat(event.castAt()).isNotNull();

        assertThat(rowsFor(alertId)).isEmpty();
        assertThat(castVoteUseCase.getUserVote(new AlertId(alertId), voter)).isEmpty();
    }

    /**
     * The committed case: the relay drains the row and the projection catches up.
     *
     * <p>Still eventually consistent — the outbox does not make the read model synchronous,
     * it makes the event unable to arrive before the vote it describes.
     */
    @Test
    void aCommittedVoteIsPublishedByTheRelay_andMarkedPublished() {
        String alertId = "alert-outbox-relay";

        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId(alertId), new VoterId("voter-outbox-relay"), VoteType.CONFIRM));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(publishedRowFor(alertId)).isPresent());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(castVoteUseCase.getVoteStats(new AlertId(alertId)).credibilityScore())
                        .isEqualTo(2));

        assertThat(publishedRowFor(alertId)).get()
                .satisfies(row -> assertThat(row.getAttempts()).isZero())
                .satisfies(row -> assertThat(row.getLastError()).isNull());
    }

    private Optional<OutboxEventJpaEntity> publishedRowFor(String alertId) {
        return rowsFor(alertId).stream()
                .filter(row -> row.getPublishedAt() != null)
                .findFirst();
    }

    /**
     * A retraction goes through the same path, so the projection returns to zero without the
     * removal event ever being able to overtake the delete it describes.
     */
    @Test
    void aRemovalUsesTheOutboxToo() {
        String alertId = "alert-outbox-removal";
        VoterId voter = new VoterId("voter-outbox-removal");

        castVoteUseCase.castVote(new CastVoteCommand(new AlertId(alertId), voter, VoteType.UPVOTE));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(castVoteUseCase.getVoteStats(new AlertId(alertId)).credibilityScore())
                        .isEqualTo(1));

        castVoteUseCase.removeVote(new AlertId(alertId), voter);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(castVoteUseCase.getVoteStats(new AlertId(alertId)).credibilityScore())
                        .isZero());

        assertThat(rowsFor(alertId))
                .extracting(OutboxEventJpaEntity::getTopic)
                .containsExactlyInAnyOrder(KafkaTopics.VOTE_CAST, KafkaTopics.VOTE_REMOVED);
    }
}

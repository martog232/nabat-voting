package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.application.projection.CredibilityProjection;
import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.in.CastVoteUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test that verifies the full Kafka event flow:
 *
 * <ol>
 *   <li>A vote is cast via {@link CastVoteUseCase}.</li>
 *   <li>A {@link com.example.nabatvoting.domain.event.VoteCastEvent} is published
 *       to the embedded Kafka broker on the {@code vote.cast} topic.</li>
 *   <li>The {@link KafkaVoteEventConsumer} receives the event and applies it to
 *       the {@link CredibilityProjection}.</li>
 *   <li>The projection reflects the updated credibility score.</li>
 * </ol>
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {KafkaTopics.VOTE_CAST},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext
class VoteKafkaIntegrationTest {

    @Autowired
    private CastVoteUseCase castVoteUseCase;

    @Autowired
    private CredibilityProjection credibilityProjection;

    @Test
    void positiveVote_updatesCredibilityProjectionViaKafka() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-kafka-1"), new VoterId("voter-kafka-A"), true);

        castVoteUseCase.castVote(command);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() ->
                        assertThat(credibilityProjection.getScore("alert-kafka-1")).isEqualTo(1));
    }

    @Test
    void negativeVote_decreasesCredibilityScore() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-kafka-2"), new VoterId("voter-kafka-B"), false);

        castVoteUseCase.castVote(command);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() ->
                        assertThat(credibilityProjection.getScore("alert-kafka-2")).isEqualTo(-1));
    }

    @Test
    void multipleVotes_accumulateCredibilityScore() {
        String alertId = "alert-kafka-3";

        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId(alertId), new VoterId("voter-1"), true));
        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId(alertId), new VoterId("voter-2"), true));
        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId(alertId), new VoterId("voter-3"), false));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() ->
                        assertThat(credibilityProjection.getScore(alertId)).isEqualTo(1));
    }
}

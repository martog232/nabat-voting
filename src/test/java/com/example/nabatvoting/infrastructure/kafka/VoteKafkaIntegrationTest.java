package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteType;
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
 * End-to-end check of the CQRS read path: casting a vote publishes a
 * {@code VoteCastEvent} to Kafka, the consumer recomputes the durable
 * {@code alert_credibility} projection, and {@link CastVoteUseCase#getVoteStats}
 * reads the resulting score back. The {@code await} blocks model the eventual
 * consistency between the write-model commit and the projection catching up.
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {KafkaTopics.VOTE_CAST, KafkaTopics.VOTE_REMOVED},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext
class VoteKafkaIntegrationTest {

    @Autowired
    private CastVoteUseCase castVoteUseCase;

    private int scoreOf(String alertId) {
        return castVoteUseCase.getVoteStats(new AlertId(alertId)).credibilityScore();
    }

    @Test
    void upvote_updatesCredibilityProjectionViaKafka() {
        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId("alert-kafka-1"), new VoterId("voter-kafka-A"), VoteType.UPVOTE));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(scoreOf("alert-kafka-1")).isEqualTo(1));
    }

    @Test
    void downvote_decreasesCredibilityScore() {
        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId("alert-kafka-2"), new VoterId("voter-kafka-B"), VoteType.DOWNVOTE));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(scoreOf("alert-kafka-2")).isEqualTo(-1));
    }

    @Test
    void confirm_addsTwoToCredibilityScore() {
        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId("alert-kafka-4"), new VoterId("voter-kafka-D"), VoteType.CONFIRM));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(scoreOf("alert-kafka-4")).isEqualTo(2));
    }

    @Test
    void removeVote_recomputesProjectionBackToZeroViaKafka() {
        String alertId = "alert-kafka-rm";
        VoterId voter = new VoterId("voter-kafka-rm");

        castVoteUseCase.castVote(new CastVoteCommand(new AlertId(alertId), voter, VoteType.UPVOTE));
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(scoreOf(alertId)).isEqualTo(1));

        castVoteUseCase.removeVote(new AlertId(alertId), voter);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(scoreOf(alertId)).isZero());
    }

    @Test
    void multipleVotes_accumulateCredibilityScore() {
        String alertId = "alert-kafka-3";

        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId(alertId), new VoterId("voter-1"), VoteType.UPVOTE));
        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId(alertId), new VoterId("voter-2"), VoteType.UPVOTE));
        castVoteUseCase.castVote(new CastVoteCommand(
                new AlertId(alertId), new VoterId("voter-3"), VoteType.DOWNVOTE));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(scoreOf(alertId)).isEqualTo(1));
    }
}

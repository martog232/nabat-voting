package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.application.projection.CredibilityProjection;
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
    void upvote_updatesCredibilityProjectionViaKafka() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-kafka-1"), new VoterId("voter-kafka-A"), VoteType.UPVOTE);

        castVoteUseCase.castVote(command);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() ->
                        assertThat(credibilityProjection.getScore("alert-kafka-1")).isEqualTo(1));
    }

    @Test
    void downvote_decreasesCredibilityScore() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-kafka-2"), new VoterId("voter-kafka-B"), VoteType.DOWNVOTE);

        castVoteUseCase.castVote(command);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() ->
                        assertThat(credibilityProjection.getScore("alert-kafka-2")).isEqualTo(-1));
    }

    @Test
    void confirm_addsTwoToCredibilityScore() {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId("alert-kafka-4"), new VoterId("voter-kafka-D"), VoteType.CONFIRM);

        castVoteUseCase.castVote(command);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() ->
                        assertThat(credibilityProjection.getScore("alert-kafka-4")).isEqualTo(2));
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
                .untilAsserted(() ->
                        assertThat(credibilityProjection.getScore(alertId)).isEqualTo(1));
    }
}

package com.example.nabatvoting.infrastructure.kafka;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import com.example.nabatvoting.domain.event.VoteRemovedEvent;
import com.example.nabatvoting.domain.event.VoteTallies;
import com.example.nabatvoting.domain.model.VoteType;
import org.example.nabat.events.vote.VoteChangeType;
import org.example.nabat.events.vote.VoteChanged;
import org.example.nabat.events.vote.VoteKind;

/**
 * Builds the wire record from the domain events.
 *
 * <p>The domain keeps two events, because casting a vote and retracting one are two different
 * things to say. The transport carries one record on one keyed topic, because the ordering
 * guarantee that matters — everything about an alert in the order it happened — is a property
 * of a single partition. This class is where the one becomes the other, and it is the only
 * place that knows both vocabularies.
 */
public final class VoteChangedEvents {

    private VoteChangedEvents() {
    }

    public static VoteChanged from(VoteCastEvent event) {
        return VoteChanged.newBuilder()
                .setChangeType(VoteChangeType.CAST)
                .setVoteId(event.voteId().toString())
                .setAlertId(event.alertId())
                .setVoterId(event.voterId())
                .setVoteType(kindOf(event.voteType()))
                .setOccurredAt(event.castAt())
                .setTallies(talliesOf(event.tallies()))
                .build();
    }

    public static VoteChanged from(VoteRemovedEvent event) {
        return VoteChanged.newBuilder()
                .setChangeType(VoteChangeType.REMOVED)
                // No vote and no vote type: a retraction is about the alert's votes, not
                // about one of them.
                .setAlertId(event.alertId())
                .setVoterId(event.voterId())
                .setOccurredAt(event.removedAt())
                .setTallies(talliesOf(event.tallies()))
                .build();
    }

    private static org.example.nabat.events.vote.VoteTallies talliesOf(VoteTallies tallies) {
        return org.example.nabat.events.vote.VoteTallies.newBuilder()
                .setUpvotes(tallies.upvotes())
                .setDownvotes(tallies.downvotes())
                .setConfirmations(tallies.confirmations())
                .setCredibilityScore(tallies.credibilityScore())
                .build();
    }

    private static VoteKind kindOf(VoteType voteType) {
        return switch (voteType) {
            case UPVOTE -> VoteKind.UPVOTE;
            case DOWNVOTE -> VoteKind.DOWNVOTE;
            case CONFIRM -> VoteKind.CONFIRM;
        };
    }
}

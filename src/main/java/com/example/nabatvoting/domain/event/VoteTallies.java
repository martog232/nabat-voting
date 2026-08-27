package com.example.nabatvoting.domain.event;

import com.example.nabatvoting.domain.model.VoteCounts;

/**
 * The alert's tallies as of the vote that produced this event.
 *
 * <p>Carried on the event so that a consumer outside this service can maintain its own
 * projection without asking. Absolute values, not deltas: at-least-once delivery makes a
 * delta wrong the second time it arrives, while writing the same absolute values twice is
 * the same write.
 *
 * <p>{@code credibilityScore} travels with them even though it is derived, because
 * {@link VoteCounts} is the single definition of that formula and a consumer that
 * recalculated it would be a second copy free to drift.
 */
public record VoteTallies(int upvotes, int downvotes, int confirmations, int credibilityScore) {

    public static VoteTallies from(VoteCounts counts) {
        return new VoteTallies(counts.upvotes(), counts.downvotes(), counts.confirmations(),
                counts.credibilityScore());
    }
}

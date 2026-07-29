package com.example.nabatvoting.domain.model;

/**
 * Raw per-type vote tallies for one alert, and the single authoritative definition
 * of how they combine into a credibility score.
 *
 * <p>The formula previously existed in three places in this service alone
 * ({@code AlertCredibility}, {@code CastVoteUseCase.VoteStats}, and the projection
 * updater's callers) plus twice more in nabat-app and the frontend. Every copy was
 * free to drift. This is the only copy on the voting side; nabat-app and the
 * frontend now consume the score rather than recomputing it.
 */
public record VoteCounts(int upvotes, int downvotes, int confirmations) {

    public static final VoteCounts EMPTY = new VoteCounts(0, 0, 0);

    public VoteCounts {
        if (upvotes < 0 || downvotes < 0 || confirmations < 0) {
            throw new IllegalArgumentException(
                    "Vote counts must not be negative: up=" + upvotes
                            + ", down=" + downvotes + ", confirm=" + confirmations);
        }
    }

    /**
     * Confirmations weigh double because they imply the voter is on-site and is
     * corroborating the report rather than just agreeing with it.
     */
    public int credibilityScore() {
        return upvotes - downvotes + (confirmations * 2);
    }
}

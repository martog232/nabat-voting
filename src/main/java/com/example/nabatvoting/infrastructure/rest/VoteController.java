package com.example.nabatvoting.infrastructure.rest;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteType;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.in.CastVoteUseCase;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

/**
 * Vote write/read API.
 *
 * <p><strong>The voter is always the authenticated principal.</strong> It is never
 * taken from the request body or a query parameter — a client-supplied voter id
 * would let any caller vote as anybody else. {@code VoteRequest.userId} exists
 * only so that a client which still sends one gets an explicit 400 rather than
 * having it silently ignored (which is what used to happen, and meant votes were
 * attributed to whichever identity the caller's token carried).
 */
@RestController
@RequestMapping("/api/v1/alerts/{alertId}/votes")
public class VoteController {

    private final CastVoteUseCase castVoteUseCase;

    public VoteController(CastVoteUseCase castVoteUseCase) {
        this.castVoteUseCase = castVoteUseCase;
    }

    @PostMapping
    public ResponseEntity<VoteResponse> castVote(
            @PathVariable UUID alertId,
            @Valid @RequestBody VoteRequest request,
            @AuthenticationPrincipal String principalUserId
    ) {
        String voterId = requireVoter(principalUserId);
        request.requireUserIdMatches(voterId);

        CastVoteCommand command = new CastVoteCommand(
                new AlertId(alertId.toString()),
                new VoterId(voterId),
                request.voteType()
        );

        CastVoteUseCase.CastVoteResult result = castVoteUseCase.castVote(command);

        return ResponseEntity
                .status(result.created() ? HttpStatus.CREATED : HttpStatus.OK)
                .body(new VoteResponse(
                        result.voteId().value(),
                        alertId,
                        request.voteType(),
                        // The persisted instant, not a fresh Instant.now() invented here.
                        result.castAt(),
                        VoteStatsResponse.from(result.stats())
                ));
    }

    /**
     * Removes the caller's vote. Returns 200 with the resulting tallies rather
     * than 204, so the caller does not have to poll the eventually-consistent
     * stats endpoint to find out the new state.
     */
    @DeleteMapping
    public ResponseEntity<VoteStatsResponse> removeVote(
            @PathVariable UUID alertId,
            @AuthenticationPrincipal String principalUserId
    ) {
        CastVoteUseCase.VoteStats stats = castVoteUseCase.removeVote(
                new AlertId(alertId.toString()),
                new VoterId(requireVoter(principalUserId))
        );
        return ResponseEntity.ok(VoteStatsResponse.from(stats));
    }

    @GetMapping("/me")
    public ResponseEntity<UserVoteResponse> myVote(
            @PathVariable UUID alertId,
            @AuthenticationPrincipal String principalUserId
    ) {
        var voteType = castVoteUseCase.getUserVote(
                new AlertId(alertId.toString()),
                new VoterId(requireVoter(principalUserId))
        );
        return ResponseEntity.ok(new UserVoteResponse(voteType.isPresent(), voteType.orElse(null)));
    }

    @GetMapping("/stats")
    public ResponseEntity<VoteStatsResponse> getStats(@PathVariable UUID alertId) {
        return ResponseEntity.ok(
                VoteStatsResponse.from(castVoteUseCase.getVoteStats(new AlertId(alertId.toString()))));
    }

    /**
     * The authenticated user id, as put into the security context by
     * {@code JwtAuthenticationFilter} from the token's {@code userId} claim.
     *
     * <p>{@link AuthenticationPrincipal} resolves to {@code null} rather than to
     * the anonymous {@link Authentication} when unauthenticated, so a null check
     * is sufficient here.
     */
    private static String requireVoter(String principalUserId) {
        if (principalUserId == null || principalUserId.isBlank()) {
            throw new BadCredentialsException("Not authenticated");
        }
        return principalUserId;
    }

    public record VoteRequest(
            @NotNull(message = "voteType is required") VoteType voteType,
            /*
             * Accepted only to be rejected. See the class javadoc.
             */
            UUID userId
    ) {
        void requireUserIdMatches(String authenticatedVoterId) {
            if (userId != null && !userId.toString().equals(authenticatedVoterId)) {
                throw new IllegalArgumentException(
                        "userId in the request body does not match the authenticated user; "
                                + "omit it — the voter is always taken from the access token");
            }
        }
    }

    public record VoteResponse(
            UUID id,
            UUID alertId,
            VoteType voteType,
            Instant createdAt,
            VoteStatsResponse stats
    ) {}

    public record UserVoteResponse(boolean hasVoted, VoteType voteType) {}

    public record VoteStatsResponse(int upvotes, int downvotes, int confirmations, int credibilityScore) {
        static VoteStatsResponse from(CastVoteUseCase.VoteStats stats) {
            return new VoteStatsResponse(
                    stats.upvotes(), stats.downvotes(), stats.confirmations(), stats.credibilityScore());
        }
    }
}

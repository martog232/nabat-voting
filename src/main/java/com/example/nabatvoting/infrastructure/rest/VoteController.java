package com.example.nabatvoting.infrastructure.rest;

import com.example.nabatvoting.domain.model.AlertId;
import com.example.nabatvoting.domain.model.VoteId;
import com.example.nabatvoting.domain.model.VoterId;
import com.example.nabatvoting.domain.port.in.CastVoteCommand;
import com.example.nabatvoting.domain.port.in.CastVoteUseCase;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/alerts/{alertId}/votes")
public class VoteController {

    private final CastVoteUseCase castVoteUseCase;

    public VoteController(CastVoteUseCase castVoteUseCase) {
        this.castVoteUseCase = castVoteUseCase;
    }

    private String currentUserId() {
        Authentication auth = org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated() || "anonymousUser".equals(auth.getPrincipal())) {
            throw new org.springframework.security.authentication.BadCredentialsException("Not authenticated");
        }
        return (String) auth.getPrincipal();
    }

    @PostMapping
    public ResponseEntity<VoteResponse> castVote(
            @PathVariable UUID alertId,
            @Valid @RequestBody VoteRequest request
    ) {
        CastVoteCommand command = new CastVoteCommand(
                new AlertId(alertId.toString()),
                new VoterId(currentUserId()),
                request.voteType()
        );
        VoteId voteId = castVoteUseCase.castVote(command);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(new VoteResponse(voteId.value(), alertId, request.voteType(), Instant.now().toString()));
    }

    @DeleteMapping
    public ResponseEntity<Void> removeVote(@PathVariable UUID alertId) {
        castVoteUseCase.removeVote(new AlertId(alertId.toString()), new VoterId(currentUserId()));
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/me")
    public ResponseEntity<UserVoteResponse> hasUserVoted(@PathVariable UUID alertId) {
        boolean hasVoted = castVoteUseCase.hasUserVoted(
                new AlertId(alertId.toString()),
                new VoterId(currentUserId())
        );
        return ResponseEntity.ok(new UserVoteResponse(hasVoted));
    }

    @GetMapping("/stats")
    public ResponseEntity<VoteStatsResponse> getStats(@PathVariable UUID alertId) {
        CastVoteUseCase.VoteStats stats = castVoteUseCase.getVoteStats(new AlertId(alertId.toString()));
        return ResponseEntity.ok(new VoteStatsResponse(
                stats.upvotes(),
                stats.downvotes(),
                stats.confirmations(),
                stats.credibilityScore()
        ));
    }

    public record VoteRequest(@NotNull com.example.nabatvoting.domain.model.VoteType voteType) {}

    public record VoteResponse(UUID id, UUID alertId, com.example.nabatvoting.domain.model.VoteType voteType, String createdAt) {}

    public record UserVoteResponse(boolean hasVoted) {}

    public record VoteStatsResponse(int upvotes, int downvotes, int confirmations, int credibilityScore) {}
}

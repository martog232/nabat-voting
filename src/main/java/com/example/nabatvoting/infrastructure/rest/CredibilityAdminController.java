package com.example.nabatvoting.infrastructure.rest;

import com.example.nabatvoting.domain.port.in.RebuildCredibilityProjectionUseCase;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Admin endpoint to replay the credibility read-model from the {@code votes}
 * write-model.
 *
 * <p>Restricted to {@code ROLE_ADMIN} both by the {@code /api/v1/admin/**} rule in
 * {@code SecurityConfig} and by the {@link PreAuthorize} below, so that moving the
 * mapping cannot silently open it up. The role comes from the {@code role} claim in
 * nabat-app's access token.
 *
 * <p>Replaying is expensive — it recomputes every alert's projection — so it is
 * deliberately not reachable by ordinary users.
 */
@RestController
@RequestMapping("/api/v1/admin/credibility")
public class CredibilityAdminController {

    private final RebuildCredibilityProjectionUseCase rebuildUseCase;

    public CredibilityAdminController(RebuildCredibilityProjectionUseCase rebuildUseCase) {
        this.rebuildUseCase = rebuildUseCase;
    }

    @PostMapping("/rebuild")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<Map<String, Object>> rebuild() {
        int rebuilt = rebuildUseCase.rebuildAll();
        return ResponseEntity.ok(Map.of("rebuiltAlerts", rebuilt));
    }
}

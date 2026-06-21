package com.example.nabatvoting.infrastructure.rest;

import com.example.nabatvoting.domain.port.in.RebuildCredibilityProjectionUseCase;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Admin endpoint to replay the credibility read-model from the {@code votes}
 * write-model.
 *
 * <p>Requires authentication (covered by {@code /api/v1/**} in SecurityConfig).
 * NOTE: tokens issued here only carry {@code ROLE_USER}; a production system
 * should restrict this to an admin role before exposing it.
 */
@RestController
@RequestMapping("/api/v1/admin/credibility")
public class CredibilityAdminController {

    private final RebuildCredibilityProjectionUseCase rebuildUseCase;

    public CredibilityAdminController(RebuildCredibilityProjectionUseCase rebuildUseCase) {
        this.rebuildUseCase = rebuildUseCase;
    }

    @PostMapping("/rebuild")
    public ResponseEntity<Map<String, Object>> rebuild() {
        int rebuilt = rebuildUseCase.rebuildAll();
        return ResponseEntity.ok(Map.of("rebuiltAlerts", rebuilt));
    }
}

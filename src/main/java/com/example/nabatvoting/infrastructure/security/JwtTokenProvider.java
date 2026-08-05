package com.example.nabatvoting.infrastructure.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Verifies access tokens minted by nabat-app.
 *
 * <p>This service does not issue tokens — it only validates them, using the same
 * shared HMAC secret. The secret validation below intentionally mirrors
 * {@code nabat-app}'s {@code JwtTokenProvider} so that a placeholder key cannot
 * slip in through whichever service happens to be deployed first.
 *
 * <p><strong>Follow-up:</strong> a shared symmetric secret means this service
 * could also <em>mint</em> tokens nabat-app would trust. Moving to RS256 with a
 * published public key would let nabat-voting verify without holding signing
 * power. Tracked separately — it changes the token format for both services.
 */
@Component
public class JwtTokenProvider {

    private static final int MIN_SECRET_LENGTH = 32;
    private static final int MIN_DISTINCT_CHARS = 16;

    private static final List<String> REJECTED_MARKERS = List.of(
            "change-me", "changeme", "local-dev", "local-docker", "development-only",
            "for-development", "placeholder", "insecure", "example", "replace-me",
            "dummy", "sample"
    );

    /** Claim names as written by nabat-app's token generator. */
    private static final String CLAIM_USER_ID = "userId";
    private static final String CLAIM_ROLE = "role";
    private static final String CLAIM_TOKEN_TYPE = "tokenType";
    private static final String ACCESS_TOKEN_TYPE = "access";

    private final SecretKey secretKey;

    public JwtTokenProvider(@Value("${jwt.secret}") String secret) {
        validateSecret(secret);
        this.secretKey = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    private static void validateSecret(String secret) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException(refusal("jwt.secret is not set"));
        }
        if (secret.length() < MIN_SECRET_LENGTH) {
            throw new IllegalStateException(refusal(
                    "jwt.secret is too short (" + secret.length() + " chars, minimum " + MIN_SECRET_LENGTH + ")"));
        }
        String lower = secret.toLowerCase(Locale.ROOT);
        for (String marker : REJECTED_MARKERS) {
            if (lower.contains(marker)) {
                throw new IllegalStateException(refusal(
                        "jwt.secret looks like a development placeholder (contains '" + marker + "')"));
            }
        }
        long distinct = secret.chars().distinct().count();
        if (distinct < MIN_DISTINCT_CHARS) {
            throw new IllegalStateException(refusal(
                    "jwt.secret has too little entropy (" + distinct + " distinct characters, minimum "
                            + MIN_DISTINCT_CHARS + ")"));
        }
    }

    private static String refusal(String problem) {
        return problem + ". Refusing to start.\n"
                + "Set the JWT_SECRET environment variable to a strong random value, e.g.\n"
                + "    openssl rand -base64 48\n"
                + "It must be the SAME secret nabat-app uses, otherwise tokens minted there "
                + "will not validate here.";
    }

    /**
     * Parses and verifies {@code token} in a single pass, returning the
     * authenticated principal only when the signature is valid <em>and</em> the
     * token is an access token.
     *
     * <p>Refresh tokens are rejected: they are long-lived (7 days by default) and
     * are only meant to be redeemed at nabat-app's {@code /auth/refresh}.
     */
    public Optional<AuthenticatedUser> authenticate(String token) {
        try {
            Claims claims = Jwts.parser()
                    .verifyWith(secretKey)
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();

            if (!ACCESS_TOKEN_TYPE.equals(claims.get(CLAIM_TOKEN_TYPE, String.class))) {
                return Optional.empty();
            }

            String userId = claims.get(CLAIM_USER_ID, String.class);
            if (userId == null || userId.isBlank()) {
                return Optional.empty();
            }

            String role = claims.get(CLAIM_ROLE, String.class);
            return Optional.of(new AuthenticatedUser(userId, role == null ? "USER" : role));
        } catch (JwtException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    /** The subset of token claims this service acts on. */
    public record AuthenticatedUser(String userId, String role) {
    }
}

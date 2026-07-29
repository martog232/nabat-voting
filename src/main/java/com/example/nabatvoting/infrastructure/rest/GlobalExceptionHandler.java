package com.example.nabatvoting.infrastructure.rest;

import com.example.nabatvoting.domain.exception.DuplicateVoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps exceptions to HTTP responses.
 *
 * <p>Client-facing messages are curated constants rather than {@code ex.getMessage()}:
 * the raw messages carried internal identifiers, and the two services were
 * inconsistent about it (nabat-app curated, this one echoed). Validation failures
 * are the one exception — the per-field messages are written for the client and
 * are what makes a 400 actionable.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    static final String MSG_UNAUTHORIZED = "Authentication required";
    static final String MSG_FORBIDDEN = "Access denied";
    static final String MSG_CONFLICT = "A vote already exists for this voter and alert";
    static final String MSG_INVALID_REQUEST = "Invalid request";
    static final String MSG_VALIDATION = "Validation failed";
    static final String MSG_INTERNAL = "Internal server error";

    @ExceptionHandler(BadCredentialsException.class)
    public ResponseEntity<ErrorResponse> handleBadCredentials(BadCredentialsException ex) {
        log.debug("BadCredentials: {}", ex.getMessage());
        return build(HttpStatus.UNAUTHORIZED, MSG_UNAUTHORIZED);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ErrorResponse> handleAccessDenied(AccessDeniedException ex) {
        log.warn("AccessDenied: {}", ex.getMessage());
        return build(HttpStatus.FORBIDDEN, MSG_FORBIDDEN);
    }

    @ExceptionHandler(DuplicateVoteException.class)
    public ResponseEntity<ErrorResponse> handleDuplicateVote(DuplicateVoteException ex) {
        log.debug("DuplicateVote: {}", ex.getMessage());
        return build(HttpStatus.CONFLICT, MSG_CONFLICT);
    }

    /** Safety net for the (alert_id, voter_id) unique constraint under a concurrent double-vote. */
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ErrorResponse> handleDataIntegrity(DataIntegrityViolationException ex) {
        log.warn("DataIntegrityViolation treated as vote conflict: {}", ex.getMessage());
        return build(HttpStatus.CONFLICT, MSG_CONFLICT);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex) {
        // Safe to surface: these are contract violations raised deliberately by our own
        // controllers (e.g. a body userId that disagrees with the token) and the message
        // tells the caller how to fix the request.
        log.debug("IllegalArgument: {}", ex.getMessage());
        return build(HttpStatus.BAD_REQUEST, ex.getMessage() == null ? MSG_INVALID_REQUEST : ex.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ValidationErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new HashMap<>();
        ex.getBindingResult().getAllErrors().forEach(error -> {
            // Class-level constraints produce ObjectError, not FieldError — an unchecked
            // cast here would turn a 400 into a 500.
            String field = error instanceof FieldError fieldError ? fieldError.getField() : error.getObjectName();
            errors.put(field, error.getDefaultMessage());
        });
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ValidationErrorResponse(
                HttpStatus.BAD_REQUEST.value(), MSG_VALIDATION, errors, Instant.now()));
    }

    /**
     * Catch-all so unexpected failures return the same JSON envelope as everything
     * else instead of Spring's default error body.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {
        log.error("Unhandled exception", ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, MSG_INTERNAL);
    }

    private ResponseEntity<ErrorResponse> build(HttpStatus status, String message) {
        return ResponseEntity.status(status)
                .body(new ErrorResponse(status.value(), status.getReasonPhrase(), message, Instant.now()));
    }

    public record ErrorResponse(int status, String error, String message, Instant timestamp) {}

    public record ValidationErrorResponse(
            int status,
            String message,
            Map<String, String> errors,
            Instant timestamp
    ) {}
}

package com.example.nabatvoting.domain.exception;

/**
 * Raised when a voter casts the exact same vote they already hold on an alert.
 * A genuine no-op, surfaced to the client as 409 Conflict rather than being
 * silently accepted or blowing up as a 500.
 */
public class DuplicateVoteException extends RuntimeException {

    public DuplicateVoteException(String message) {
        super(message);
    }
}

package com.fde.txnprocessor.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mutable processing state for one request (one row in a production state table).
 * Keeps a full status history so every record is traceable end-to-end.
 *
 * <p>Not thread-safe by itself: it is only mutated by the engine under its lock
 * (in production: a row lock / optimistic version column).
 */
public final class ProcessingRecord {

    /** One entry in the status history (audit trail). */
    public record StatusChange(TransactionStatus status, Instant at, String note) { }

    private final String requestId;
    private final String transactionId;
    private final TransactionRequest request; // null for records rejected by validation
    private final String sourceRef;
    private final List<StatusChange> history = new ArrayList<>();

    private TransactionStatus status;
    private int attempts;
    private FailureReason failureReason;
    private String detail;
    private Instant nextAttemptAt;

    private ProcessingRecord(String requestId, String transactionId, TransactionRequest request,
                             String sourceRef, Instant receivedAt) {
        this.requestId = requestId;
        this.transactionId = transactionId;
        this.request = request;
        this.sourceRef = sourceRef;
        this.status = TransactionStatus.RECEIVED;
        this.history.add(new StatusChange(TransactionStatus.RECEIVED, receivedAt, sourceRef));
    }

    public static ProcessingRecord received(TransactionRequest request, String sourceRef, Instant at) {
        return new ProcessingRecord(request.requestId(), request.transactionId(), request, sourceRef, at);
    }

    /** A record that failed validation; identifiers are best-effort (may be null/invalid). */
    public static ProcessingRecord rejected(String rawRequestId, String rawTransactionId, String sourceRef, Instant at) {
        return new ProcessingRecord(rawRequestId, rawTransactionId, null, sourceRef, at);
    }

    /** Moves to the next status, enforcing the lifecycle state machine. */
    public void transitionTo(TransactionStatus next, Instant at, String note) {
        if (!status.canTransitionTo(next)) {
            throw new IllegalStateException("Illegal transition " + status + " -> " + next
                    + " for request " + requestId);
        }
        this.status = next;
        this.history.add(new StatusChange(next, at, note));
        if (next != TransactionStatus.FAILED) {
            this.failureReason = null;
        }
        this.detail = note;
    }

    public void fail(FailureReason reason, Instant at, String note) {
        transitionTo(TransactionStatus.FAILED, at, note);
        this.failureReason = reason;
    }

    public int incrementAttempts() {
        return ++attempts;
    }

    public void resetAttempts() {
        attempts = 0;
    }

    public String lifecycle() {
        return history.stream().map(c -> c.status().name()).collect(Collectors.joining(" -> "));
    }

    public String requestId() { return requestId; }
    public String transactionId() { return transactionId; }
    public TransactionRequest request() { return request; }
    public String sourceRef() { return sourceRef; }
    public TransactionStatus status() { return status; }
    public int attempts() { return attempts; }
    public FailureReason failureReason() { return failureReason; }
    public String detail() { return detail; }
    public Instant nextAttemptAt() { return nextAttemptAt; }
    public void nextAttemptAt(Instant at) { this.nextAttemptAt = at; }
    public List<StatusChange> history() { return Collections.unmodifiableList(history); }
}

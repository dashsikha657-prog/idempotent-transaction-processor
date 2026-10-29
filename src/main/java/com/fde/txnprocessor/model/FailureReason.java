package com.fde.txnprocessor.model;

/** Why a record ended in FAILED. Drives operator action and whether redrive is allowed. */
public enum FailureReason {
    /** Malformed or invalid input. Fix at source and resend with a new requestId. */
    VALIDATION_ERROR(false),
    /** Same transactionId / requestId re-used with a different payload. Needs investigation. */
    IDEMPOTENCY_CONFLICT(false),
    /** Sequence number already consumed or occupied by a different transaction. */
    SEQUENCE_CONFLICT(false),
    /** Debit or reversal would overdraw the account. */
    INSUFFICIENT_FUNDS(false),
    /** Reversal references an unknown, mismatched or already-reversed transaction. */
    INVALID_REVERSAL(false),
    /** Transient failures persisted beyond the retry budget. Safe to redrive. */
    RETRY_EXHAUSTED(true),
    /** Unclassified error. Isolated so the batch continues; investigate logs. */
    UNEXPECTED_ERROR(false);

    private final boolean redrivable;

    FailureReason(boolean redrivable) {
        this.redrivable = redrivable;
    }

    public boolean isRedrivable() {
        return redrivable;
    }
}

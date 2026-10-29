package com.fde.txnprocessor.ledger;

/** A ledger invariant would be violated; the posting was NOT applied (non-retryable). */
public class LedgerConstraintException extends RuntimeException {

    public enum Constraint { INSUFFICIENT_FUNDS, ALREADY_REVERSED }

    private final Constraint constraint;

    public LedgerConstraintException(Constraint constraint, String message) {
        super(message);
        this.constraint = constraint;
    }

    public Constraint constraint() {
        return constraint;
    }
}

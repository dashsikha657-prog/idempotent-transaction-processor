package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.model.FailureReason;

/** A deterministic business rejection — retrying would give the same answer, so we fail fast. */
public class BusinessRuleException extends RuntimeException {

    private final FailureReason reason;

    public BusinessRuleException(FailureReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public FailureReason reason() {
        return reason;
    }
}

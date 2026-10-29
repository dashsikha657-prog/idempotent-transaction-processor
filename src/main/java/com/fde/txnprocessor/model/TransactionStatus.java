package com.fde.txnprocessor.model;

import java.util.EnumSet;
import java.util.Set;

/**
 * Lifecycle of a single transaction request.
 *
 * <pre>
 * RECEIVED ──► PROCESSING ──► PROCESSED
 *    │  ▲          │  ▲
 *    │  │          ▼  │
 *    │  │     RETRY_PENDING ──► FAILED (retry exhausted) ──(operator redrive)──► RETRY_PENDING
 *    │  └── PENDING (waiting for earlier sequence)
 *    ├──► DUPLICATE
 *    └──► FAILED (validation / conflict)
 * </pre>
 *
 * Transitions are enforced: an illegal transition is a programming error and throws.
 */
public enum TransactionStatus {
    RECEIVED,
    PENDING,
    PROCESSING,
    RETRY_PENDING,
    PROCESSED,
    DUPLICATE,
    FAILED;

    public boolean isTerminal() {
        return this == PROCESSED || this == DUPLICATE || this == FAILED;
    }

    public boolean canTransitionTo(TransactionStatus next) {
        return allowedNext().contains(next);
    }

    private Set<TransactionStatus> allowedNext() {
        return switch (this) {
            case RECEIVED -> EnumSet.of(PENDING, PROCESSING, DUPLICATE, FAILED);
            case PENDING -> EnumSet.of(PROCESSING, FAILED);
            case PROCESSING -> EnumSet.of(PROCESSED, RETRY_PENDING, FAILED);
            case RETRY_PENDING -> EnumSet.of(PROCESSING, FAILED);
            // FAILED is terminal for automatic processing; only an operator redrive
            // of a retry-exhausted record may move it back to RETRY_PENDING.
            case FAILED -> EnumSet.of(RETRY_PENDING);
            case PROCESSED, DUPLICATE -> EnumSet.noneOf(TransactionStatus.class);
        };
    }
}

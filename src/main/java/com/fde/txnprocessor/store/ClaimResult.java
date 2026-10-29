package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.ProcessingRecord;

/**
 * Result of atomically claiming a request in the state store.
 *
 * @param kind    what the store found
 * @param record  NEW / DUPLICATE_OF: the newly created record for this request;
 *                REPLAY: the existing record for this requestId
 * @param owner   DUPLICATE_OF: the record that already owns the transactionId; otherwise null
 */
public record ClaimResult(Kind kind, ProcessingRecord record, ProcessingRecord owner) {

    public enum Kind {
        /** First time this requestId and this transactionId are seen: proceed to processing. */
        NEW,
        /** The exact requestId was seen before (client retry / at-least-once redelivery). */
        REPLAY,
        /** New requestId, but the transactionId is already owned by another request. */
        DUPLICATE_OF
    }

    static ClaimResult newRecord(ProcessingRecord r) { return new ClaimResult(Kind.NEW, r, null); }
    static ClaimResult replay(ProcessingRecord existing) { return new ClaimResult(Kind.REPLAY, existing, null); }
    static ClaimResult duplicateOf(ProcessingRecord created, ProcessingRecord owner) {
        return new ClaimResult(Kind.DUPLICATE_OF, created, owner);
    }
}

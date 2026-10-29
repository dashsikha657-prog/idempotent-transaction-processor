package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.ProcessingRecord;
import com.fde.txnprocessor.model.TransactionRequest;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Idempotency / processing-state store (L2-FR-03).
 *
 * <p>Production mapping: a {@code transaction_request} table with UNIQUE(request_id) and a
 * {@code transaction_claim} table with UNIQUE(transaction_id). {@link #claim} is then a single
 * {@code INSERT ... ON CONFLICT DO NOTHING} pair inside one DB transaction, which is what makes
 * duplicate detection safe across multiple concurrent workers.
 */
public interface TransactionStateStore {

    /** Atomically registers the request and claims its transactionId. */
    ClaimResult claim(TransactionRequest request, String sourceRef, Instant at);

    /** Stores a record that failed validation (kept separate so bad input never poisons idempotency keys). */
    void addRejected(ProcessingRecord record);

    Optional<ProcessingRecord> findByRequestId(String requestId);

    Optional<ProcessingRecord> findByTransactionId(String transactionId);

    /** All records (accepted and rejected) in arrival order. */
    List<ProcessingRecord> all();
}

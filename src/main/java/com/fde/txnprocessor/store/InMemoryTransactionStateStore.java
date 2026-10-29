package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.ProcessingRecord;
import com.fde.txnprocessor.model.TransactionRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory implementation. All mutations go through one monitor, which plays the role of the
 * database's unique constraints + transaction: the check-and-insert in {@link #claim} is atomic,
 * so two concurrent deliveries of the same request can never both be treated as NEW.
 */
public final class InMemoryTransactionStateStore implements TransactionStateStore {

    private final Map<String, ProcessingRecord> byRequestId = new HashMap<>();
    private final Map<String, String> transactionOwner = new HashMap<>(); // transactionId -> requestId
    private final List<ProcessingRecord> arrivalOrder = new ArrayList<>();

    @Override
    public synchronized ClaimResult claim(TransactionRequest request, String sourceRef, Instant at) {
        ProcessingRecord existing = byRequestId.get(request.requestId());
        if (existing != null) {
            return ClaimResult.replay(existing);
        }
        ProcessingRecord created = ProcessingRecord.received(request, sourceRef, at);
        byRequestId.put(request.requestId(), created);
        arrivalOrder.add(created);

        String ownerRequestId = transactionOwner.putIfAbsent(request.transactionId(), request.requestId());
        if (ownerRequestId != null) {
            return ClaimResult.duplicateOf(created, byRequestId.get(ownerRequestId));
        }
        return ClaimResult.newRecord(created);
    }

    @Override
    public synchronized void addRejected(ProcessingRecord record) {
        arrivalOrder.add(record);
    }

    @Override
    public synchronized Optional<ProcessingRecord> findByRequestId(String requestId) {
        return Optional.ofNullable(byRequestId.get(requestId));
    }

    @Override
    public synchronized Optional<ProcessingRecord> findByTransactionId(String transactionId) {
        String owner = transactionOwner.get(transactionId);
        return owner == null ? Optional.empty() : Optional.ofNullable(byRequestId.get(owner));
    }

    @Override
    public synchronized List<ProcessingRecord> all() {
        return List.copyOf(arrivalOrder);
    }
}

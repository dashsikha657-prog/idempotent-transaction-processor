package com.fde.txnprocessor.observability;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters for operational visibility. In production these map 1:1 to Micrometer counters
 * (e.g. txn_processed_total{outcome="duplicate"}) scraped by Prometheus.
 */
public final class ProcessingMetrics {

    public final AtomicLong deliveriesReceived = new AtomicLong();
    public final AtomicLong rejectedByValidation = new AtomicLong();
    public final AtomicLong replayedDeliveries = new AtomicLong();
    public final AtomicLong duplicatesDetected = new AtomicLong();
    public final AtomicLong idempotencyConflicts = new AtomicLong();
    public final AtomicLong parkedOutOfOrder = new AtomicLong();
    public final AtomicLong processingAttempts = new AtomicLong();
    public final AtomicLong retriesScheduled = new AtomicLong();
    public final AtomicLong processed = new AtomicLong();
    public final AtomicLong idempotentLedgerNoOps = new AtomicLong();
    public final AtomicLong failed = new AtomicLong();
    public final AtomicLong redrives = new AtomicLong();

    public Map<String, Long> snapshot() {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("deliveriesReceived", deliveriesReceived.get());
        m.put("rejectedByValidation", rejectedByValidation.get());
        m.put("replayedDeliveries", replayedDeliveries.get());
        m.put("duplicatesDetected", duplicatesDetected.get());
        m.put("idempotencyConflicts", idempotencyConflicts.get());
        m.put("parkedOutOfOrder", parkedOutOfOrder.get());
        m.put("processingAttempts", processingAttempts.get());
        m.put("retriesScheduled", retriesScheduled.get());
        m.put("processed", processed.get());
        m.put("idempotentLedgerNoOps", idempotentLedgerNoOps.get());
        m.put("failed", failed.get());
        m.put("redrives", redrives.get());
        return m;
    }
}

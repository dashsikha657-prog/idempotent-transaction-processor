package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.ingest.RawRecord;
import com.fde.txnprocessor.ingest.TransactionSource;
import com.fde.txnprocessor.ledger.Ledger;
import com.fde.txnprocessor.ledger.PostingResult;
import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.ProcessingRecord;
import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.observability.EventLogger;
import com.fde.txnprocessor.observability.Masking;
import com.fde.txnprocessor.observability.ProcessingMetrics;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.store.ClaimResult;
import com.fde.txnprocessor.store.TransactionStateStore;
import com.fde.txnprocessor.validation.RequestValidator;
import com.fde.txnprocessor.validation.ValidationResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Stream;

/**
 * The background worker. Orchestrates the pipeline for each delivered record:
 *
 * <pre>
 *  raw record ─► validate ─► claim (idempotency) ─► sequence gate ─► process (ledger) ─► state + metrics
 *                  │              │                       │                 │
 *                FAILED      DUPLICATE / replay         PENDING     RETRY_PENDING ─► retry queue
 * </pre>
 *
 * <p>The engine models a single consumer per stream partition (like one Kafka consumer per
 * partition), so its public operations are serialized. The stores and ledger it uses are
 * independently safe for concurrent access.
 */
public final class ProcessingEngine {

    private final RequestValidator validator;
    private final TransactionStateStore store;
    private final SequenceGate gate;
    private final OrderingMode orderingMode;
    private final TransactionProcessor processor;
    private final Ledger ledger;
    private final RetryPolicy retryPolicy;
    private final Clock clock;
    private final Sleeper sleeper;
    private final EventLogger log;
    private final ProcessingMetrics metrics = new ProcessingMetrics();

    private record RetryTask(Instant dueAt, long order, String requestId) { }

    private final PriorityQueue<RetryTask> retryQueue = new PriorityQueue<>(
            Comparator.comparing(RetryTask::dueAt).thenComparingLong(RetryTask::order));
    private long retryOrder;

    public ProcessingEngine(RequestValidator validator, TransactionStateStore store, SequenceGate gate,
                            OrderingMode orderingMode, TransactionProcessor processor, Ledger ledger,
                            RetryPolicy retryPolicy, Clock clock, Sleeper sleeper, EventLogger log) {
        this.validator = validator;
        this.store = store;
        this.gate = gate;
        this.orderingMode = orderingMode;
        this.processor = processor;
        this.ledger = ledger;
        this.retryPolicy = retryPolicy;
        this.clock = clock;
        this.sleeper = sleeper;
        this.log = log;
    }

    /** Runs one batch: ingest everything from the source, then drain scheduled retries. */
    public ProcessingReport run(TransactionSource source) {
        log.info("BATCH_STARTED", "source", source.name());
        try (Stream<RawRecord> records = source.read()) {
            records.forEach(this::ingest);
        }
        drainRetries();
        ProcessingReport report = report(source.name());
        log.info("BATCH_COMPLETED", "source", source.name(),
                "processed", String.valueOf(report.count(TransactionStatus.PROCESSED)),
                "duplicate", String.valueOf(report.count(TransactionStatus.DUPLICATE)),
                "failed", String.valueOf(report.count(TransactionStatus.FAILED)),
                "pending", String.valueOf(report.count(TransactionStatus.PENDING)));
        return report;
    }

    /** Handles one delivered record. Never throws for bad data: every record is isolated. */
    public synchronized void ingest(RawRecord raw) {
        metrics.deliveriesReceived.incrementAndGet();
        Instant now = clock.instant();
        ValidationResult v;
        try {
            v = validator.validate(raw);
        } catch (RuntimeException e) {
            v = new ValidationResult(null, java.util.List.of("validator error: " + e.getClass().getSimpleName()),
                    null, null, null, java.util.OptionalLong.empty());
        }

        if (!v.isValid()) {
            rejectInvalid(raw, v, now);
            return;
        }

        TransactionRequest request = v.request();
        ClaimResult claim = store.claim(request, raw.sourceRef(), now);
        switch (claim.kind()) {
            case REPLAY -> onReplay(request, claim.record());
            case DUPLICATE_OF -> onDuplicate(request, claim.record(), claim.owner(), now);
            case NEW -> admit(claim.record(), now);
        }
    }

    // ------------------------------------------------------------------ ingestion outcomes

    private void rejectInvalid(RawRecord raw, ValidationResult v, Instant now) {
        metrics.rejectedByValidation.incrementAndGet();
        metrics.failed.incrementAndGet();
        ProcessingRecord rec = ProcessingRecord.rejected(v.rawRequestId(), v.rawTransactionId(), raw.sourceRef(), now);
        rec.fail(FailureReason.VALIDATION_ERROR, now, String.join("; ", v.errors()));
        store.addRejected(rec);
        log.warn("REJECTED_INVALID", "requestId", v.rawRequestId(), "transactionId", v.rawTransactionId(),
                "source", raw.sourceRef(), "errors", String.join("; ", v.errors()));

        // Close its sequence slot so one bad record cannot stall the stream (see SequenceGate).
        String partition = orderingMode.partitionOf(v.rawAccountId());
        if (v.bestEffortSequence().isPresent() && partition != null) {
            gate.closeWithoutProcessing(partition, v.bestEffortSequence().getAsLong())
                    .ifPresent(this::processChainFrom);
        }
    }

    private void onReplay(TransactionRequest request, ProcessingRecord existing) {
        metrics.replayedDeliveries.incrementAndGet();
        boolean samePayload = existing.request() != null
                && existing.request().fingerprint().equals(request.fingerprint());
        if (samePayload) {
            log.info("REPLAY_IGNORED", "requestId", request.requestId(), "transactionId", request.transactionId(),
                    "currentStatus", existing.status().name());
        } else {
            metrics.idempotencyConflicts.incrementAndGet();
            log.error("REPLAY_PAYLOAD_MISMATCH", "requestId", request.requestId(),
                    "transactionId", request.transactionId(), "currentStatus", existing.status().name(),
                    "action", "ignored; original kept");
        }
    }

    private void onDuplicate(TransactionRequest request, ProcessingRecord rec, ProcessingRecord owner, Instant now) {
        boolean samePayload = owner.request().fingerprint().equals(request.fingerprint());
        if (samePayload) {
            metrics.duplicatesDetected.incrementAndGet();
            rec.transitionTo(TransactionStatus.DUPLICATE, now,
                    "duplicate of " + owner.requestId() + " (owner status " + owner.status() + ")");
            log.info("DUPLICATE_DETECTED", "requestId", request.requestId(), "transactionId", request.transactionId(),
                    "duplicateOf", owner.requestId(), "ownerStatus", owner.status().name());
        } else {
            metrics.idempotencyConflicts.incrementAndGet();
            metrics.failed.incrementAndGet();
            rec.fail(FailureReason.IDEMPOTENCY_CONFLICT, now,
                    "transactionId already used by " + owner.requestId() + " with a different payload");
            log.error("IDEMPOTENCY_CONFLICT", "requestId", request.requestId(),
                    "transactionId", request.transactionId(), "ownerRequestId", owner.requestId());
        }
    }

    private void admit(ProcessingRecord rec, Instant now) {
        TransactionRequest r = rec.request();
        String partition = orderingMode.partitionOf(r);
        switch (gate.admit(partition, r.sequenceNumber(), r.requestId())) {
            case READY -> processChainFrom(rec);
            case PARKED -> {
                metrics.parkedOutOfOrder.incrementAndGet();
                rec.transitionTo(TransactionStatus.PENDING, now, "waiting for earlier sequence in partition " + partition);
                log.info("PARKED_OUT_OF_ORDER", "requestId", r.requestId(), "transactionId", r.transactionId(),
                        "sequence", String.valueOf(r.sequenceNumber()), "partition", partition);
            }
            case STALE, CONFLICT -> {
                metrics.failed.incrementAndGet();
                rec.fail(FailureReason.SEQUENCE_CONFLICT, now,
                        "sequence " + r.sequenceNumber() + " already consumed or held by another transaction");
                log.error("SEQUENCE_CONFLICT", "requestId", r.requestId(), "transactionId", r.transactionId(),
                        "sequence", String.valueOf(r.sequenceNumber()), "partition", partition);
            }
        }
    }

    // ------------------------------------------------------------------ processing

    private void processChainFrom(String requestId) {
        store.findByRequestId(requestId).ifPresent(this::processChainFrom);
    }

    /**
     * Processes the record, then — while each one reaches a terminal outcome — any parked
     * successors the gate releases. Iterative (not recursive) so long backlogs can't blow the stack.
     */
    private void processChainFrom(ProcessingRecord first) {
        Deque<ProcessingRecord> work = new ArrayDeque<>();
        work.add(first);
        while (!work.isEmpty()) {
            ProcessingRecord rec = work.poll();
            TransactionStatus outcome = attempt(rec);
            if (outcome.isTerminal()) {
                gate.complete(orderingMode.partitionOf(rec.request()), rec.request().sequenceNumber())
                        .flatMap(store::findByRequestId)
                        .ifPresent(work::add);
            }
        }
    }

    /** One processing attempt. Returns the resulting status. */
    private TransactionStatus attempt(ProcessingRecord rec) {
        TransactionRequest r = rec.request();
        Instant now = clock.instant();
        int attemptNo = rec.incrementAttempts();
        metrics.processingAttempts.incrementAndGet();
        rec.transitionTo(TransactionStatus.PROCESSING, now, "attempt " + attemptNo);
        try {
            PostingResult result = processor.apply(r);
            String note;
            if (result.alreadyApplied()) {
                metrics.idempotentLedgerNoOps.incrementAndGet();
                note = "ledger entry #" + result.entry().entryNo() + " already existed (idempotent no-op)";
            } else {
                note = "ledger entry #" + result.entry().entryNo() + " posted";
            }
            rec.transitionTo(TransactionStatus.PROCESSED, clock.instant(), note);
            metrics.processed.incrementAndGet();
            log.info("PROCESSED", "requestId", r.requestId(), "transactionId", r.transactionId(),
                    "type", r.type().name(), "account", Masking.account(r.accountId()),
                    "attempt", String.valueOf(attemptNo), "ledgerEntry", String.valueOf(result.entry().entryNo()),
                    "idempotentNoOp", String.valueOf(result.alreadyApplied()));
            return TransactionStatus.PROCESSED;

        } catch (TransientFailureException e) {
            if (retryPolicy.canRetryAfter(attemptNo)) {
                Duration delay = retryPolicy.delayAfter(attemptNo);
                Instant due = clock.instant().plus(delay);
                rec.nextAttemptAt(due);
                rec.transitionTo(TransactionStatus.RETRY_PENDING, clock.instant(),
                        "transient failure on attempt " + attemptNo + ": " + e.getMessage());
                retryQueue.add(new RetryTask(due, retryOrder++, r.requestId()));
                metrics.retriesScheduled.incrementAndGet();
                log.warn("RETRY_SCHEDULED", "requestId", r.requestId(), "transactionId", r.transactionId(),
                        "attempt", String.valueOf(attemptNo), "maxAttempts", String.valueOf(retryPolicy.maxAttempts()),
                        "delayMs", String.valueOf(delay.toMillis()), "cause", e.getMessage());
                return TransactionStatus.RETRY_PENDING;
            }
            rec.fail(FailureReason.RETRY_EXHAUSTED, clock.instant(),
                    "gave up after " + attemptNo + " attempts; last error: " + e.getMessage());
            metrics.failed.incrementAndGet();
            log.error("FAILED_RETRY_EXHAUSTED", "requestId", r.requestId(), "transactionId", r.transactionId(),
                    "attempts", String.valueOf(attemptNo), "cause", e.getMessage());
            return TransactionStatus.FAILED;

        } catch (BusinessRuleException e) {
            rec.fail(e.reason(), clock.instant(), e.getMessage());
            metrics.failed.incrementAndGet();
            log.error("FAILED_BUSINESS_RULE", "requestId", r.requestId(), "transactionId", r.transactionId(),
                    "reason", e.reason().name(), "message", e.getMessage());
            return TransactionStatus.FAILED;

        } catch (RuntimeException e) {
            // Safe error handling: record the error type only (messages may contain data), isolate, continue.
            rec.fail(FailureReason.UNEXPECTED_ERROR, clock.instant(), "unexpected " + e.getClass().getSimpleName());
            metrics.failed.incrementAndGet();
            log.error("FAILED_UNEXPECTED", "requestId", r.requestId(), "transactionId", r.transactionId(),
                    "errorType", e.getClass().getName());
            return TransactionStatus.FAILED;
        }
    }

    // ------------------------------------------------------------------ retries & recovery

    /** Waits for and executes scheduled retries until none remain. */
    public void drainRetries() {
        while (true) {
            RetryTask task;
            synchronized (this) {
                task = retryQueue.poll();
            }
            if (task == null) {
                return;
            }
            Duration wait = Duration.between(clock.instant(), task.dueAt());
            if (!wait.isNegative() && !wait.isZero()) {
                sleeper.sleep(wait);
            }
            synchronized (this) {
                store.findByRequestId(task.requestId())
                        .filter(rec -> rec.status() == TransactionStatus.RETRY_PENDING)
                        .ifPresent(this::processChainFrom);
            }
        }
    }

    /**
     * Operator recovery: re-run a record that FAILED with RETRY_EXHAUSTED (e.g. after the downstream
     * outage is fixed). Safe because the ledger posting is idempotent on transactionId.
     *
     * @return true if the record was re-queued
     */
    public synchronized boolean redrive(String requestId) {
        Optional<ProcessingRecord> found = store.findByRequestId(requestId);
        if (found.isEmpty()) {
            return false;
        }
        ProcessingRecord rec = found.get();
        if (rec.status() != TransactionStatus.FAILED || rec.failureReason() == null
                || !rec.failureReason().isRedrivable()) {
            log.warn("REDRIVE_REFUSED", "requestId", requestId, "status", rec.status().name(),
                    "reason", rec.failureReason() == null ? "-" : rec.failureReason().name());
            return false;
        }
        rec.resetAttempts();
        rec.transitionTo(TransactionStatus.RETRY_PENDING, clock.instant(), "operator redrive");
        Instant due = clock.instant();
        rec.nextAttemptAt(due);
        retryQueue.add(new RetryTask(due, retryOrder++, requestId));
        metrics.redrives.incrementAndGet();
        log.info("REDRIVE_QUEUED", "requestId", requestId, "transactionId", rec.transactionId());
        return true;
    }

    public synchronized ProcessingReport report(String sourceName) {
        return ProcessingReport.build(sourceName, store.all(), gate.gaps(), ledger.balances(),
                ledger.entries().size(), metrics);
    }

    public ProcessingMetrics metrics() {
        return metrics;
    }
}

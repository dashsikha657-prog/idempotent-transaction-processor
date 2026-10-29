package com.fde.txnprocessor.simulation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Declares which transactions should experience simulated transient failures and how many times.
 *
 * <ul>
 *   <li><b>before commit</b> — e.g. DB timeout before the write: nothing was applied.</li>
 *   <li><b>after commit</b> — e.g. the write succeeded but the acknowledgement was lost. This is
 *       the dangerous case for idempotency: the retry must NOT post a second time.</li>
 * </ul>
 */
public final class FaultPlan {

    private final Map<String, AtomicInteger> beforeCommit = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> afterCommit = new ConcurrentHashMap<>();

    public static FaultPlan none() {
        return new FaultPlan();
    }

    public FaultPlan failBeforeCommit(String transactionId, int times) {
        beforeCommit.put(transactionId, new AtomicInteger(times));
        return this;
    }

    public FaultPlan failAfterCommit(String transactionId, int times) {
        afterCommit.put(transactionId, new AtomicInteger(times));
        return this;
    }

    boolean shouldFailBeforeCommit(String transactionId) {
        return consume(beforeCommit, transactionId);
    }

    boolean shouldFailAfterCommit(String transactionId) {
        return consume(afterCommit, transactionId);
    }

    public boolean isEmpty() {
        return beforeCommit.isEmpty() && afterCommit.isEmpty();
    }

    private static boolean consume(Map<String, AtomicInteger> plan, String transactionId) {
        AtomicInteger remaining = plan.get(transactionId);
        return remaining != null && remaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0;
    }
}

package com.fde.txnprocessor;

import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.processing.RetryPolicy;
import com.fde.txnprocessor.simulation.FaultPlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static com.fde.txnprocessor.TestSupport.raw;
import static com.fde.txnprocessor.TestSupport.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryAndRecoveryTest {

    @Test
    @DisplayName("Transient failure twice, success on 3rd attempt (= max) -> PROCESSED, posted once")
    void succeedsOnLastAllowedAttempt() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-1", 2));
        t.engine.run(source(raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100")));

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-1").status());
        assertEquals(3, t.record("REQ-1").attempts());
        assertEquals(1, t.ledger.entries().size());
        assertEquals("RECEIVED -> PROCESSING -> RETRY_PENDING -> PROCESSING -> RETRY_PENDING -> PROCESSING -> PROCESSED",
                t.record("REQ-1").lifecycle());
    }

    @Test
    @DisplayName("Retry exhaustion -> FAILED(RETRY_EXHAUSTED), and the next sequence is still processed")
    void exhaustionFailsAndStreamContinues() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-1", 99));
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"),
                raw("TXN-2", "REQ-2", 2, "ACC-2", "CREDIT", "5")));

        assertEquals(TransactionStatus.FAILED, t.record("REQ-1").status());
        assertEquals(FailureReason.RETRY_EXHAUSTED, t.record("REQ-1").failureReason());
        assertEquals(3, t.record("REQ-1").attempts());
        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-2").status());
        t.assertBalance("ACC-1", "0");
    }

    @Test
    @DisplayName("Backoff is exponential: 100ms then 200ms between attempts")
    void exponentialBackoff() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-1", 2));
        t.engine.run(source(raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "1")));

        assertEquals(2, t.sleeps.size());
        assertTrue(t.sleeps.get(0).toMillis() <= 100 && t.sleeps.get(0).toMillis() > 50);
        assertTrue(t.sleeps.get(1).toMillis() <= 200 && t.sleeps.get(1).toMillis() > 150);

        RetryPolicy p = new RetryPolicy(10, Duration.ofMillis(100), Duration.ofMillis(500));
        assertEquals(List.of(100L, 200L, 400L, 500L, 500L),
                List.of(p.delayAfter(1), p.delayAfter(2), p.delayAfter(3), p.delayAfter(4), p.delayAfter(5))
                        .stream().map(Duration::toMillis).toList());
    }

    @Test
    @DisplayName("Operator redrive of a retry-exhausted record succeeds once the dependency recovers")
    void redriveAfterExhaustion() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-1", 3)); // exactly the budget
        t.engine.run(source(raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100")));
        assertEquals(FailureReason.RETRY_EXHAUSTED, t.record("REQ-1").failureReason());

        assertTrue(t.engine.redrive("REQ-1"));
        t.engine.drainRetries();

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-1").status());
        assertEquals(1, t.ledger.entries().size());
        t.assertBalance("ACC-1", "100");
        assertTrue(t.record("REQ-1").lifecycle().contains("FAILED -> RETRY_PENDING -> PROCESSING -> PROCESSED"));
    }

    @Test
    @DisplayName("Redrive after an after-commit failure exhaustion still posts only once")
    void redriveIsIdempotentEvenIfFirstAttemptsCommitted() {
        TestSupport t = new TestSupport(FaultPlan.none().failAfterCommit("TXN-1", 3));
        t.engine.run(source(raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100")));
        assertEquals(TransactionStatus.FAILED, t.record("REQ-1").status());
        t.assertBalance("ACC-1", "100"); // it actually committed on attempt 1

        t.engine.redrive("REQ-1");
        t.engine.drainRetries();

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-1").status());
        assertEquals(1, t.ledger.entries().size());
        t.assertBalance("ACC-1", "100");
    }

    @Test
    @DisplayName("Business failures are not retried and cannot be redriven")
    void businessFailuresFailFast() {
        TestSupport t = new TestSupport();
        t.engine.run(source(raw("TXN-1", "REQ-1", 1, "ACC-1", "DEBIT", "10")));

        assertEquals(FailureReason.INSUFFICIENT_FUNDS, t.record("REQ-1").failureReason());
        assertEquals(1, t.record("REQ-1").attempts());
        assertEquals(0, t.engine.metrics().retriesScheduled.get());
        assertFalse(t.engine.redrive("REQ-1"));
        assertFalse(t.engine.redrive("NO-SUCH-REQUEST"));
    }
}

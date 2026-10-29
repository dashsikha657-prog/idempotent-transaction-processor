package com.fde.txnprocessor;

import com.fde.txnprocessor.ledger.InMemoryLedger;
import com.fde.txnprocessor.ledger.LedgerEntry;
import com.fde.txnprocessor.ledger.PostingResult;
import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.model.TransactionType;
import com.fde.txnprocessor.simulation.FaultPlan;
import com.fde.txnprocessor.store.ClaimResult;
import com.fde.txnprocessor.store.InMemoryTransactionStateStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.fde.txnprocessor.TestSupport.raw;
import static com.fde.txnprocessor.TestSupport.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotencyTest {

    @Test
    @DisplayName("Exact redelivery (same requestId) is ignored and posts nothing")
    void replayOfSameRequestIdIsIgnored() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"),
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"),
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100")));

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-1").status());
        assertEquals(1, t.ledger.entries().size());
        t.assertBalance("ACC-1", "100");
        assertEquals(2, t.engine.metrics().replayedDeliveries.get());
    }

    @Test
    @DisplayName("Duplicate (new requestId, same transactionId) arriving AFTER processing -> DUPLICATE")
    void duplicateAfterProcessing() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"),
                raw("TXN-1", "REQ-1-RETRY", 1, "ACC-1", "CREDIT", "100")));

        assertEquals(TransactionStatus.DUPLICATE, t.record("REQ-1-RETRY").status());
        assertEquals(1, t.ledger.entries().size());
        t.assertBalance("ACC-1", "100");
    }

    @Test
    @DisplayName("Duplicate arriving while the original is still PENDING is caught, and the original still runs once")
    void duplicateWhileOriginalPending() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-2", "REQ-2", 2, "ACC-1", "CREDIT", "50"),       // parked
                raw("TXN-2", "REQ-2-DUP", 2, "ACC-1", "CREDIT", "50"),   // duplicate of a pending one
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100")));    // releases seq 2

        assertEquals(TransactionStatus.DUPLICATE, t.record("REQ-2-DUP").status());
        assertTrue(t.record("REQ-2-DUP").detail().contains("PENDING"));
        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-2").status());
        assertEquals(2, t.ledger.entries().size());
        t.assertBalance("ACC-1", "150");
    }

    @Test
    @DisplayName("Same transactionId with a DIFFERENT payload is a conflict, never a silent duplicate or overwrite")
    void conflictingPayloadIsFlagged() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"),
                raw("TXN-1", "REQ-1-B", 1, "ACC-1", "CREDIT", "9999")));

        assertEquals(TransactionStatus.FAILED, t.record("REQ-1-B").status());
        assertEquals(FailureReason.IDEMPOTENCY_CONFLICT, t.record("REQ-1-B").failureReason());
        t.assertBalance("ACC-1", "100");
        assertEquals(1, t.engine.metrics().idempotencyConflicts.get());
    }

    @Test
    @DisplayName("Lost acknowledgement AFTER commit: the retry is an idempotent no-op, balance applied once")
    void retryAfterCommitDoesNotDoublePost() {
        TestSupport t = new TestSupport(FaultPlan.none().failAfterCommit("TXN-1", 1));
        t.engine.run(source(raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100")));

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-1").status());
        assertEquals(2, t.record("REQ-1").attempts());
        assertEquals(1, t.ledger.entries().size());
        t.assertBalance("ACC-1", "100");
        assertEquals(1, t.engine.metrics().idempotentLedgerNoOps.get());
        assertTrue(t.record("REQ-1").detail().contains("idempotent no-op"));
    }

    @Test
    @DisplayName("Ledger: 16 threads posting the same transactionId concurrently -> exactly one entry")
    void concurrentPostingOfSameTransactionAppliesOnce() throws Exception {
        InMemoryLedger ledger = new InMemoryLedger();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<PostingResult>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return ledger.post(LedgerEntry.draft("TXN-1", "ACC-1", "INR", new BigDecimal("100"), null, 1));
            }));
        }
        start.countDown();
        int applied = 0;
        for (Future<PostingResult> f : results) {
            if (!f.get(5, TimeUnit.SECONDS).alreadyApplied()) {
                applied++;
            }
        }
        pool.shutdown();

        assertEquals(1, applied);
        assertEquals(1, ledger.entries().size());
        assertEquals(0, ledger.balance("ACC-1", "INR").compareTo(new BigDecimal("100")));
    }

    @Test
    @DisplayName("State store: concurrent claims of the same transactionId -> exactly one NEW owner")
    void concurrentClaimsHaveSingleOwner() throws Exception {
        InMemoryTransactionStateStore store = new InMemoryTransactionStateStore();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ClaimResult>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String reqId = "REQ-" + i;
            results.add(pool.submit(() -> {
                start.await();
                return store.claim(new TransactionRequest("TXN-1", reqId, "ACC-1", TransactionType.CREDIT,
                        BigDecimal.TEN, "INR", 1, null, null), "test", Instant.now());
            }));
        }
        start.countDown();
        int owners = 0;
        int duplicates = 0;
        for (Future<ClaimResult> f : results) {
            switch (f.get(5, TimeUnit.SECONDS).kind()) {
                case NEW -> owners++;
                case DUPLICATE_OF -> duplicates++;
                default -> { }
            }
        }
        pool.shutdown();

        assertEquals(1, owners);
        assertEquals(threads - 1, duplicates);
    }

    @Test
    @DisplayName("Fingerprint ignores requestId and eventTime but not amount")
    void fingerprintSemantics() {
        TransactionRequest a = new TransactionRequest("TXN-1", "REQ-1", "ACC-1", TransactionType.DEBIT,
                new BigDecimal("40"), "INR", 2, Instant.parse("2026-01-01T00:00:00Z"), null);
        TransactionRequest sameBusinessTxn = new TransactionRequest("TXN-1", "REQ-OTHER", "ACC-1",
                TransactionType.DEBIT, new BigDecimal("40.00"), "INR", 2, null, null);
        TransactionRequest differentAmount = new TransactionRequest("TXN-1", "REQ-1", "ACC-1",
                TransactionType.DEBIT, new BigDecimal("41"), "INR", 2, null, null);

        assertEquals(a.fingerprint(), sameBusinessTxn.fingerprint());
        assertTrue(!a.fingerprint().equals(differentAmount.fingerprint()));
    }
}

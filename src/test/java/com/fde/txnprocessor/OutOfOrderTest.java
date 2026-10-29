package com.fde.txnprocessor;

import com.fde.txnprocessor.ledger.LedgerEntry;
import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.processing.OrderingMode;
import com.fde.txnprocessor.processing.SequenceGate;
import com.fde.txnprocessor.simulation.FaultPlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.fde.txnprocessor.TestSupport.raw;
import static com.fde.txnprocessor.TestSupport.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutOfOrderTest {

    @Test
    @DisplayName("Fully reversed arrival order is applied in sequence order")
    void reverseArrivalIsAppliedInSequenceOrder() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-4", "REQ-4", 4, "ACC-1", "CREDIT", "4"),
                raw("TXN-3", "REQ-3", 3, "ACC-1", "CREDIT", "3"),
                raw("TXN-2", "REQ-2", 2, "ACC-1", "CREDIT", "2"),
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "1")));

        List<Long> postedOrder = t.ledger.entries().stream().map(LedgerEntry::sequenceNumber).toList();
        assertEquals(List.of(1L, 2L, 3L, 4L), postedOrder);
        assertEquals(3, t.engine.metrics().parkedOutOfOrder.get());
        t.assertBalance("ACC-1", "10");
    }

    @Test
    @DisplayName("A debit arriving before the credit it depends on waits instead of failing for funds")
    void debitWaitsForEarlierCredit() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-2", "REQ-2", 2, "ACC-1", "DEBIT", "40"),
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100")));

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-2").status());
        t.assertBalance("ACC-1", "60");
    }

    @Test
    @DisplayName("A missing sequence leaves later requests PENDING and is reported as a gap")
    void missingSequenceIsReported() {
        TestSupport t = new TestSupport();
        ProcessingReport report = t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "1"),
                raw("TXN-3", "REQ-3", 3, "ACC-1", "CREDIT", "3"),
                raw("TXN-5", "REQ-5", 5, "ACC-1", "CREDIT", "5")));

        assertEquals(TransactionStatus.PENDING, t.record("REQ-3").status());
        assertEquals(TransactionStatus.PENDING, t.record("REQ-5").status());
        assertEquals(2, report.count(TransactionStatus.PENDING));
        SequenceGate.Gap gap = report.gaps().get(0);
        assertEquals(2, gap.nextExpected());
        assertEquals(List.of(3L, 5L), gap.parked());
        assertEquals(List.of(2L, 4L), gap.missing());
    }

    @Test
    @DisplayName("The gap closes when the missing request finally arrives (later batch / later poll)")
    void gapClosesWhenMissingArrives() {
        TestSupport t = new TestSupport();
        t.engine.run(source(raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "1"),
                raw("TXN-3", "REQ-3", 3, "ACC-1", "CREDIT", "3")));
        assertEquals(TransactionStatus.PENDING, t.record("REQ-3").status());

        ProcessingReport report = t.engine.run(source(raw("TXN-2", "REQ-2", 2, "ACC-1", "CREDIT", "2")));

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-3").status());
        assertTrue(report.gaps().isEmpty());
        t.assertBalance("ACC-1", "6");
    }

    @Test
    @DisplayName("An invalid record with a readable sequence closes its slot so the stream keeps moving")
    void invalidRecordDoesNotBlockStream() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-3", "REQ-3", 3, "ACC-1", "CREDIT", "3"),     // parked behind 2
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "1"),
                raw("TXN-2", "REQ-2", 2, "ACC-1", "CREDIT", "abc"))); // invalid -> closes slot 2

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-3").status());
    }

    @Test
    @DisplayName("A different transaction re-using an already consumed sequence is a SEQUENCE_CONFLICT")
    void staleSequenceIsRejected() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "1"),
                raw("TXN-X", "REQ-X", 1, "ACC-1", "CREDIT", "7")));

        assertEquals(TransactionStatus.FAILED, t.record("REQ-X").status());
        assertEquals(FailureReason.SEQUENCE_CONFLICT, t.record("REQ-X").failureReason());
        t.assertBalance("ACC-1", "1");
    }

    @Test
    @DisplayName("A record that is RETRY_PENDING holds its slot: later sequences wait for it (ordering preserved)")
    void retryingRecordHoldsItsSlot() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-1", 1));
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"), // fails once, retried in drain phase
                raw("TXN-2", "REQ-2", 2, "ACC-1", "DEBIT", "60")));  // must not run before the credit

        assertEquals("RECEIVED -> PENDING -> PROCESSING -> PROCESSED", t.record("REQ-2").lifecycle());
        List<Long> postedOrder = t.ledger.entries().stream().map(LedgerEntry::sequenceNumber).toList();
        assertEquals(List.of(1L, 2L), postedOrder);
        t.assertBalance("ACC-1", "40");
    }

    @Test
    @DisplayName("PER_ACCOUNT ordering: a gap on one account does not block another account")
    void perAccountPartitionsAreIndependent() {
        TestSupport t = new TestSupport(FaultPlan.none(), OrderingMode.PER_ACCOUNT, 3);
        t.engine.run(source(
                raw("TXN-A2", "REQ-A2", 2, "ACC-A", "CREDIT", "2"),   // ACC-A seq 1 missing
                raw("TXN-B1", "REQ-B1", 1, "ACC-B", "CREDIT", "1"),
                raw("TXN-B2", "REQ-B2", 2, "ACC-B", "CREDIT", "2")));

        assertEquals(TransactionStatus.PENDING, t.record("REQ-A2").status());
        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-B1").status());
        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-B2").status());
    }
}

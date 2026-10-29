package com.fde.txnprocessor;

import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.ProcessingRecord;
import com.fde.txnprocessor.model.TransactionStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static com.fde.txnprocessor.TestSupport.raw;
import static com.fde.txnprocessor.TestSupport.reversal;
import static com.fde.txnprocessor.TestSupport.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BusinessRulesTest {

    @Test
    @DisplayName("Debit that would overdraw -> FAILED(INSUFFICIENT_FUNDS), balance untouched")
    void overdraftRejected() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "50"),
                raw("TXN-2", "REQ-2", 2, "ACC-1", "DEBIT", "50.01")));

        assertEquals(FailureReason.INSUFFICIENT_FUNDS, t.record("REQ-2").failureReason());
        t.assertBalance("ACC-1", "50");
    }

    @Test
    @DisplayName("Full reversal of a credit restores the balance; a second reversal is rejected")
    void reversalRules() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"),
                reversal("TXN-R1", "REQ-R1", 2, "ACC-1", "100", "TXN-1"),
                reversal("TXN-R2", "REQ-R2", 3, "ACC-1", "100", "TXN-1")));

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-R1").status());
        t.assertBalance("ACC-1", "0");
        assertEquals(FailureReason.INVALID_REVERSAL, t.record("REQ-R2").failureReason());
    }

    @Test
    @DisplayName("Reversal of a debit credits the money back")
    void reversalOfDebit() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"),
                raw("TXN-2", "REQ-2", 2, "ACC-1", "DEBIT", "30"),
                reversal("TXN-3", "REQ-3", 3, "ACC-1", "30", "TXN-2")));

        t.assertBalance("ACC-1", "100");
    }

    @Test
    @DisplayName("Reversal of unknown transaction, wrong account or partial amount is rejected")
    void invalidReversals() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"),
                reversal("TXN-R1", "REQ-R1", 2, "ACC-1", "100", "TXN-NOPE"),
                reversal("TXN-R2", "REQ-R2", 3, "ACC-2", "100", "TXN-1"),
                reversal("TXN-R3", "REQ-R3", 4, "ACC-1", "40", "TXN-1")));

        assertEquals(FailureReason.INVALID_REVERSAL, t.record("REQ-R1").failureReason());
        assertEquals(FailureReason.INVALID_REVERSAL, t.record("REQ-R2").failureReason());
        assertEquals(FailureReason.INVALID_REVERSAL, t.record("REQ-R3").failureReason());
        t.assertBalance("ACC-1", "100");
    }

    @Test
    @DisplayName("Status lifecycle is enforced: illegal transitions throw")
    void stateMachineIsEnforced() {
        ProcessingRecord r = ProcessingRecord.rejected("REQ-1", "TXN-1", "test", Instant.now());

        assertThrows(IllegalStateException.class, () -> r.transitionTo(TransactionStatus.PROCESSED, Instant.now(), "x"));
        r.transitionTo(TransactionStatus.PROCESSING, Instant.now(), "attempt 1");
        r.transitionTo(TransactionStatus.PROCESSED, Instant.now(), "done");
        assertThrows(IllegalStateException.class, () -> r.transitionTo(TransactionStatus.PROCESSING, Instant.now(), "again"));
        assertThrows(IllegalStateException.class, () -> r.transitionTo(TransactionStatus.DUPLICATE, Instant.now(), "x"));

        assertTrue(TransactionStatus.PROCESSED.isTerminal());
        assertTrue(TransactionStatus.DUPLICATE.isTerminal());
        assertFalse(TransactionStatus.RETRY_PENDING.isTerminal());
        assertTrue(TransactionStatus.FAILED.canTransitionTo(TransactionStatus.RETRY_PENDING));
        assertFalse(TransactionStatus.DUPLICATE.canTransitionTo(TransactionStatus.PROCESSING));
    }
}

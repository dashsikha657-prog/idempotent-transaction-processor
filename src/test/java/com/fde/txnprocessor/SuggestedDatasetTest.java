package com.fde.txnprocessor;

import com.fde.txnprocessor.ingest.CsvTransactionSource;
import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.simulation.FaultPlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static com.fde.txnprocessor.TestSupport.raw;
import static com.fde.txnprocessor.TestSupport.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The exact L2 test data from section 9.8 of the assessment, in the given input order. */
class SuggestedDatasetTest {

    private static com.fde.txnprocessor.ingest.InMemoryTransactionSource suggestedData() {
        return source(
                raw("TXN-2002", "REQ-2", 2, "ACC-201", "DEBIT", "40"),
                raw("TXN-2001", "REQ-1", 1, "ACC-201", "CREDIT", "100"),
                raw("TXN-2002", "REQ-2-DUP", 2, "ACC-201", "DEBIT", "40"),
                raw("TXN-2003", "REQ-3", 3, "ACC-201", "CREDIT", "25"),
                raw("TXN-2004", "REQ-4", 4, "ACC-202", "DEBIT", "-10"),
                raw("TXN-2005", "REQ-5", 5, "ACC-203", "CREDIT", "500"));
    }

    @Test
    @DisplayName("9.8: every row reaches the expected outcome when TXN-2005 fails once then succeeds")
    void suggestedDataRetryOnceThenProcessed() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-2005", 1));

        ProcessingReport report = t.engine.run(suggestedData());

        // Row 1: DEBIT seq 2 arrives before seq 1 -> parked (PENDING), then processed once seq 1 lands
        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-2").status());
        assertEquals("RECEIVED -> PENDING -> PROCESSING -> PROCESSED", t.record("REQ-2").lifecycle());
        // Row 2: CREDIT processed
        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-1").status());
        // Row 3: same transactionId, new requestId -> DUPLICATE, no second debit
        assertEquals(TransactionStatus.DUPLICATE, t.record("REQ-2-DUP").status());
        // Row 4: processed
        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-3").status());
        // Row 5: negative amount -> FAILED validation, batch continues
        assertEquals(1, report.count(TransactionStatus.FAILED));
        assertEquals("VALIDATION_ERROR", report.records().stream()
                .filter(l -> l.requestId().equals("REQ-4")).findFirst().orElseThrow().failureReason());
        // Row 6: retry once, then processed
        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-5").status());
        assertEquals(2, t.record("REQ-5").attempts());

        // Business outcome: 100 - 40 + 25 = 85, and exactly 4 postings (no duplicate debit)
        t.assertBalance("ACC-201", "85");
        t.assertBalance("ACC-203", "500");
        assertEquals(4, t.ledger.entries().size());

        assertEquals(4, report.count(TransactionStatus.PROCESSED));
        assertEquals(1, report.count(TransactionStatus.DUPLICATE));
        assertEquals(0, report.count(TransactionStatus.PENDING));
        assertTrue(report.gaps().isEmpty());
    }

    @Test
    @DisplayName("9.8 variant: TXN-2005 keeps failing -> FAILED with RETRY_EXHAUSTED, nothing posted for it")
    void suggestedDataRetryExhausted() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-2005", 10));

        t.engine.run(suggestedData());

        assertEquals(TransactionStatus.FAILED, t.record("REQ-5").status());
        assertEquals(FailureReason.RETRY_EXHAUSTED, t.record("REQ-5").failureReason());
        assertEquals(3, t.record("REQ-5").attempts());
        t.assertBalance("ACC-203", "0");
        t.assertBalance("ACC-201", "85");
        assertEquals(3, t.ledger.entries().size());
    }

    @Test
    @DisplayName("Same data read from the shipped CSV file produces the same result")
    void suggestedDataFromCsvFile() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-2005", 1));

        ProcessingReport report = t.engine.run(new CsvTransactionSource(Path.of("data/sample-transactions.csv")));

        assertEquals(4, report.count(TransactionStatus.PROCESSED));
        assertEquals(1, report.count(TransactionStatus.DUPLICATE));
        assertEquals(1, report.count(TransactionStatus.FAILED));
        t.assertBalance("ACC-201", "85");
    }

    @Test
    @DisplayName("Running the same batch twice (full replay) changes nothing the second time")
    void wholeBatchReplayIsIdempotent() {
        TestSupport t = new TestSupport();
        t.engine.run(suggestedData());
        int entriesAfterFirstRun = t.ledger.entries().size();

        t.engine.run(suggestedData());

        assertEquals(entriesAfterFirstRun, t.ledger.entries().size());
        t.assertBalance("ACC-201", "85");
        t.assertBalance("ACC-203", "500");
        assertEquals(5, t.engine.metrics().replayedDeliveries.get()); // 5 valid rows replayed; REQ-4 re-rejected
    }
}

package com.fde.txnprocessor;

import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.observability.EventLogger;
import com.fde.txnprocessor.observability.Masking;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.simulation.FaultPlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.fde.txnprocessor.TestSupport.raw;
import static com.fde.txnprocessor.TestSupport.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObservabilityTest {

    @Test
    @DisplayName("Every outcome emits a named structured event")
    void eventsForEveryOutcome() {
        TestSupport t = new TestSupport(FaultPlan.none().failBeforeCommit("TXN-3", 9));
        t.engine.run(source(
                raw("TXN-2", "REQ-2", 2, "ACC-1", "CREDIT", "1"),
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "1"),
                raw("TXN-1", "REQ-1-D", 1, "ACC-1", "CREDIT", "1"),
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "1"),
                raw("TXN-9", "REQ-9", 9, "ACC-1", "CREDIT", "-1"),
                raw("TXN-3", "REQ-3", 3, "ACC-1", "CREDIT", "1")));

        Set<String> names = t.log.events().stream().map(EventLogger.Event::name).collect(Collectors.toSet());
        assertTrue(names.containsAll(List.of("BATCH_STARTED", "PARKED_OUT_OF_ORDER", "PROCESSED", "DUPLICATE_DETECTED",
                "REPLAY_IGNORED", "REJECTED_INVALID", "RETRY_SCHEDULED", "FAILED_RETRY_EXHAUSTED", "BATCH_COMPLETED")),
                "missing events, got " + names);
    }

    @Test
    @DisplayName("Logs never contain the raw account id or the amount")
    void logsAreMasked() {
        TestSupport t = new TestSupport();
        t.engine.run(source(raw("TXN-1", "REQ-1", 1, "ACC-998877", "CREDIT", "12345.67")));

        for (EventLogger.Event e : t.log.events()) {
            assertTrue(!e.line().contains("ACC-998877"), "raw account leaked: " + e.line());
            assertTrue(!e.line().contains("12345.67"), "amount leaked: " + e.line());
        }
        assertEquals("*******877", Masking.account("ACC-998877"));
        assertEquals("****201:INR", Masking.balanceKey("ACC-201:INR"));
    }

    @Test
    @DisplayName("Report: counts by status, masked balances, text and JSON renderings")
    void reportContents() {
        TestSupport t = new TestSupport();
        ProcessingReport report = t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-201", "CREDIT", "100"),
                raw("TXN-1", "REQ-1-D", 1, "ACC-201", "CREDIT", "100"),
                raw("TXN-3", "REQ-3", 3, "ACC-201", "CREDIT", "1")));

        assertEquals(1, report.count(TransactionStatus.PROCESSED));
        assertEquals(1, report.count(TransactionStatus.DUPLICATE));
        assertEquals(1, report.count(TransactionStatus.PENDING));
        assertTrue(report.maskedBalances().containsKey("****201:INR"));

        String text = report.toText();
        assertTrue(text.contains("PROCESSING REPORT"));
        assertTrue(text.contains("missing=[2]"));
        assertTrue(!text.contains("ACC-201"));

        String json = report.toJson();
        assertTrue(json.contains("\"PROCESSED\": 1"));
        assertTrue(json.contains("\"lifecycle\": \"RECEIVED -> PENDING\""));
    }
}

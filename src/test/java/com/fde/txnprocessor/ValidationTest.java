package com.fde.txnprocessor;

import com.fde.txnprocessor.ingest.CsvTransactionSource;
import com.fde.txnprocessor.ingest.RawRecord;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.validation.RequestValidator;
import com.fde.txnprocessor.validation.ValidationResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.fde.txnprocessor.TestSupport.raw;
import static com.fde.txnprocessor.TestSupport.rawWith;
import static com.fde.txnprocessor.TestSupport.reversal;
import static com.fde.txnprocessor.TestSupport.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidationTest {

    private final RequestValidator validator = new RequestValidator();

    private void assertInvalid(RawRecord r, String expectedErrorFragment) {
        ValidationResult v = validator.validate(r);
        assertFalse(v.isValid(), "expected invalid: " + expectedErrorFragment);
        assertTrue(v.errors().stream().anyMatch(e -> e.contains(expectedErrorFragment)),
                "expected an error containing '" + expectedErrorFragment + "' but got " + v.errors());
    }

    @Test
    @DisplayName("Each malformed field is rejected with a specific message")
    void rejectsMalformedFields() {
        assertInvalid(rawWith(null, "R", "1", "A", "CREDIT", "1", "INR", null), "transactionId is required");
        assertInvalid(rawWith("T", null, "1", "A", "CREDIT", "1", "INR", null), "requestId is required");
        assertInvalid(rawWith("T", "R", "1", null, "CREDIT", "1", "INR", null), "accountId is required");
        assertInvalid(rawWith("T", "R", "1", "A", "TRANSFER", "1", "INR", null), "transactionType must be one of");
        assertInvalid(rawWith("T", "R", "1", "A", "CREDIT", "-10", "INR", null), "greater than zero");
        assertInvalid(rawWith("T", "R", "1", "A", "CREDIT", "0", "INR", null), "greater than zero");
        assertInvalid(rawWith("T", "R", "1", "A", "CREDIT", "ten", "INR", null), "decimal number");
        assertInvalid(rawWith("T", "R", "1", "A", "CREDIT", "1.001", "INR", null), "at most 2 decimal places");
        assertInvalid(rawWith("T", "R", "1", "A", "CREDIT", "99999999999", "INR", null), "exceeds");
        assertInvalid(rawWith("T", "R", "1", "A", "CREDIT", "1", "rupees", null), "3-letter");
        assertInvalid(rawWith("T", "R", "0", "A", "CREDIT", "1", "INR", null), "positive integer");
        assertInvalid(rawWith("T", "R", "x", "A", "CREDIT", "1", "INR", null), "positive integer");
        assertInvalid(rawWith("T", "R", null, "A", "CREDIT", "1", "INR", null), "sequenceNumber is required");
        assertInvalid(rawWith("T;DROP", "R", "1", "A", "CREDIT", "1", "INR", null), "invalid format");
        assertInvalid(rawWith("T", "R", "1", "A", "REVERSAL", "1", "INR", null), "required for REVERSAL");
        assertInvalid(rawWith("T", "R", "1", "A", "CREDIT", "1", "INR", "T0"), "only allowed for REVERSAL");
        assertInvalid(reversal("T", "R", 1, "A", "1", "T"), "cannot reverse itself");
        assertInvalid(RawRecord.unparseable("x", "bad row"), "unparseable");
    }

    @Test
    @DisplayName("A valid record passes and keeps all fields")
    void acceptsValidRecord() {
        ValidationResult v = validator.validate(raw("TXN-1", "REQ-1", 7, "ACC-1", "credit", "10.50"));
        assertTrue(v.isValid());
        assertEquals(7, v.request().sequenceNumber());
        assertEquals("CREDIT", v.request().type().name());
    }

    @Test
    @DisplayName("Error messages never echo the raw input value")
    void errorsDoNotEchoInput() {
        ValidationResult v = validator.validate(
                rawWith("T", "R", "1", "A", "CREDIT", "SECRET-1234-5678", "INR", null));
        assertTrue(v.errors().stream().noneMatch(e -> e.contains("SECRET")));
    }

    @Test
    @DisplayName("Invalid records are isolated: the rest of the batch is processed")
    void invalidRecordsDoNotStopBatch() {
        TestSupport t = new TestSupport();
        ProcessingReport report = t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "10"),
                raw("TXN-2", "REQ-2", 2, "ACC-1", "CREDIT", "-5"),
                RawRecord.unparseable("file:line 9", "expected 9 columns but found 2"),
                raw("TXN-3", "REQ-3", 3, "ACC-1", "CREDIT", "20")));

        assertEquals(2, report.count(TransactionStatus.PROCESSED));
        assertEquals(2, report.count(TransactionStatus.FAILED));
        t.assertBalance("ACC-1", "30");
    }

    @Test
    @DisplayName("An invalid record never claims an idempotency key: a corrected resend with the same ids works")
    void invalidRecordDoesNotPoisonIdempotencyKeys() {
        TestSupport t = new TestSupport();
        t.engine.run(source(
                raw("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "-5"),
                raw("TXN-1", "REQ-1", 2, "ACC-1", "CREDIT", "5")));

        assertEquals(TransactionStatus.PROCESSED, t.record("REQ-1").status());
        t.assertBalance("ACC-1", "5");
    }

    @Test
    @DisplayName("CSV source: comments/blank lines skipped, short rows become isolated parse errors")
    void csvSourceParsing() throws Exception {
        Path tmp = Files.createTempFile("txn", ".csv");
        Files.write(tmp, List.of(
                "# comment",
                "transactionId,requestId,sequenceNumber,accountId,transactionType,amount,currency",
                "",
                "TXN-1,REQ-1,1,ACC-1,CREDIT,10,INR",
                "TXN-2,REQ-2,2",
                "TXN-3,REQ-3,3,ACC-1,CREDIT,5,INR"));
        try {
            List<RawRecord> records = new CsvTransactionSource(tmp).read().toList();
            assertEquals(3, records.size());
            assertEquals("REQ-1", records.get(0).get(RawRecord.REQUEST_ID));
            assertTrue(records.get(1).parseError().contains("expected 7 columns"));
            assertTrue(records.get(1).sourceRef().endsWith("line 5"));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}

package com.fde.txnprocessor.ingest;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An unvalidated record exactly as delivered by an input source (all values are strings).
 * Keeping ingestion "dumb" means every source (file, queue, API) shares one validator.
 *
 * @param sourceRef  where it came from, e.g. "sample.csv:line 4" or "queue:offset 1043"
 * @param fields     field name to raw value
 * @param parseError non-null if the source could not even split the record into fields
 */
public record RawRecord(String sourceRef, Map<String, String> fields, String parseError) {

    public static final String TRANSACTION_ID = "transactionId";
    public static final String REQUEST_ID = "requestId";
    public static final String SEQUENCE_NUMBER = "sequenceNumber";
    public static final String ACCOUNT_ID = "accountId";
    public static final String TRANSACTION_TYPE = "transactionType";
    public static final String AMOUNT = "amount";
    public static final String CURRENCY = "currency";
    public static final String EVENT_TIME = "eventTime";
    public static final String ORIGINAL_TRANSACTION_ID = "originalTransactionId";

    public RawRecord {
        fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    public static RawRecord of(String sourceRef, Map<String, String> fields) {
        return new RawRecord(sourceRef, fields, null);
    }

    public static RawRecord unparseable(String sourceRef, String error) {
        return new RawRecord(sourceRef, Map.of(), error);
    }

    public String get(String field) {
        String v = fields.get(field);
        return v == null ? null : v.trim();
    }
}

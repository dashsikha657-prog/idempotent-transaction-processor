package com.fde.txnprocessor.ingest;

import java.util.stream.Stream;

/**
 * Replaceable input port (L2-FR-01). Implementations: CSV file, in-memory list.
 * A Kafka/SQS/JMS consumer or a DB-polling job would implement the same interface;
 * the engine never knows where records came from.
 */
public interface TransactionSource {

    /** Lazily streams raw records in delivery order. Callers must close the stream. */
    Stream<RawRecord> read();

    /** Human-readable name for logs/reports. */
    String name();
}

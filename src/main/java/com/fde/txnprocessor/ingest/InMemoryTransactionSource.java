package com.fde.txnprocessor.ingest;

import java.util.List;
import java.util.stream.Stream;

/** Source backed by an in-memory list — used by tests and for embedding the engine. */
public final class InMemoryTransactionSource implements TransactionSource {

    private final List<RawRecord> records;

    public InMemoryTransactionSource(List<RawRecord> records) {
        this.records = List.copyOf(records);
    }

    @Override
    public Stream<RawRecord> read() {
        return records.stream();
    }

    @Override
    public String name() {
        return "in-memory(" + records.size() + " records)";
    }
}

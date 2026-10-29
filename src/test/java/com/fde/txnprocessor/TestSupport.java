package com.fde.txnprocessor;

import com.fde.txnprocessor.ingest.InMemoryTransactionSource;
import com.fde.txnprocessor.ingest.RawRecord;
import com.fde.txnprocessor.ledger.InMemoryLedger;
import com.fde.txnprocessor.model.ProcessingRecord;
import com.fde.txnprocessor.observability.EventLogger;
import com.fde.txnprocessor.processing.EngineBuilder;
import com.fde.txnprocessor.processing.OrderingMode;
import com.fde.txnprocessor.processing.ProcessingEngine;
import com.fde.txnprocessor.processing.RetryPolicy;
import com.fde.txnprocessor.processing.Sleeper;
import com.fde.txnprocessor.simulation.FaultPlan;
import com.fde.txnprocessor.store.InMemoryTransactionStateStore;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared fixtures: a fully wired engine with observable ledger/store/log and no real sleeping. */
public final class TestSupport {

    public final InMemoryLedger ledger = new InMemoryLedger();
    public final InMemoryTransactionStateStore store = new InMemoryTransactionStateStore();
    public final EventLogger log = EventLogger.silent(Clock.systemUTC());
    public final List<Duration> sleeps = new java.util.ArrayList<>();
    public final ProcessingEngine engine;

    public TestSupport() {
        this(FaultPlan.none(), OrderingMode.GLOBAL, 3);
    }

    public TestSupport(FaultPlan faults) {
        this(faults, OrderingMode.GLOBAL, 3);
    }

    public TestSupport(FaultPlan faults, OrderingMode mode, int maxAttempts) {
        Sleeper recordingSleeper = sleeps::add;
        this.engine = new EngineBuilder()
                .ledger(ledger)
                .store(store)
                .logger(log)
                .faultPlan(faults)
                .orderingMode(mode)
                .sleeper(recordingSleeper)
                .retryPolicy(new RetryPolicy(maxAttempts, Duration.ofMillis(100), Duration.ofSeconds(5)))
                .build();
    }

    public static RawRecord raw(String txnId, String reqId, long seq, String account, String type, String amount) {
        return rawWith(txnId, reqId, String.valueOf(seq), account, type, amount, "INR", null);
    }

    public static RawRecord reversal(String txnId, String reqId, long seq, String account, String amount,
                                     String originalTxnId) {
        return rawWith(txnId, reqId, String.valueOf(seq), account, "REVERSAL", amount, "INR", originalTxnId);
    }

    public static RawRecord rawWith(String txnId, String reqId, String seq, String account, String type,
                                    String amount, String currency, String originalTxnId) {
        Map<String, String> f = new LinkedHashMap<>();
        f.put(RawRecord.TRANSACTION_ID, txnId);
        f.put(RawRecord.REQUEST_ID, reqId);
        f.put(RawRecord.SEQUENCE_NUMBER, seq);
        f.put(RawRecord.ACCOUNT_ID, account);
        f.put(RawRecord.TRANSACTION_TYPE, type);
        f.put(RawRecord.AMOUNT, amount);
        f.put(RawRecord.CURRENCY, currency);
        f.put(RawRecord.ORIGINAL_TRANSACTION_ID, originalTxnId);
        return RawRecord.of("test:" + reqId, f);
    }

    public static InMemoryTransactionSource source(RawRecord... records) {
        return new InMemoryTransactionSource(List.of(records));
    }

    public ProcessingRecord record(String requestId) {
        return store.findByRequestId(requestId)
                .orElseThrow(() -> new AssertionError("no record for " + requestId));
    }

    public BigDecimal balance(String account) {
        return ledger.balance(account, "INR");
    }

    public static BigDecimal amt(String v) {
        return new BigDecimal(v);
    }

    /** Scale-insensitive money assertion (100 == 100.00). */
    public void assertBalance(String account, String expected) {
        BigDecimal actual = balance(account);
        if (actual.compareTo(new BigDecimal(expected)) != 0) {
            throw new AssertionError("balance of " + account + " expected " + expected + " but was " + actual);
        }
    }
}

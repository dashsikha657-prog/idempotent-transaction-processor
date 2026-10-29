package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.ledger.InMemoryLedger;
import com.fde.txnprocessor.ledger.Ledger;
import com.fde.txnprocessor.observability.EventLogger;
import com.fde.txnprocessor.simulation.FaultInjectingLedger;
import com.fde.txnprocessor.simulation.FaultPlan;
import com.fde.txnprocessor.store.InMemoryTransactionStateStore;
import com.fde.txnprocessor.store.TransactionStateStore;
import com.fde.txnprocessor.validation.RequestValidator;

import java.time.Clock;

/** Composition root: wires the in-memory adapters. Swap adapters here for DB / queue versions. */
public final class EngineBuilder {

    private Clock clock = Clock.systemUTC();
    private Sleeper sleeper = Sleeper.REAL;
    private RetryPolicy retryPolicy = RetryPolicy.defaults();
    private OrderingMode orderingMode = OrderingMode.GLOBAL;
    private FaultPlan faultPlan = FaultPlan.none();
    private EventLogger logger;
    private Ledger ledger;
    private TransactionStateStore store;

    public EngineBuilder clock(Clock clock) { this.clock = clock; return this; }
    public EngineBuilder sleeper(Sleeper sleeper) { this.sleeper = sleeper; return this; }
    public EngineBuilder retryPolicy(RetryPolicy policy) { this.retryPolicy = policy; return this; }
    public EngineBuilder orderingMode(OrderingMode mode) { this.orderingMode = mode; return this; }
    public EngineBuilder faultPlan(FaultPlan plan) { this.faultPlan = plan; return this; }
    public EngineBuilder logger(EventLogger logger) { this.logger = logger; return this; }
    public EngineBuilder ledger(Ledger ledger) { this.ledger = ledger; return this; }
    public EngineBuilder store(TransactionStateStore store) { this.store = store; return this; }

    public ProcessingEngine build() {
        Ledger realLedger = ledger != null ? ledger : new InMemoryLedger(clock);
        Ledger effectiveLedger = faultPlan.isEmpty() ? realLedger : new FaultInjectingLedger(realLedger, faultPlan);
        return new ProcessingEngine(
                new RequestValidator(),
                store != null ? store : new InMemoryTransactionStateStore(),
                new SequenceGate(),
                orderingMode,
                new TransactionProcessor(effectiveLedger),
                realLedger,
                retryPolicy,
                clock,
                sleeper,
                logger != null ? logger : EventLogger.toStderr(clock));
    }
}

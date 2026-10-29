package com.fde.txnprocessor.simulation;

import com.fde.txnprocessor.ledger.Ledger;
import com.fde.txnprocessor.ledger.LedgerEntry;
import com.fde.txnprocessor.ledger.PostingResult;
import com.fde.txnprocessor.processing.TransientFailureException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Decorator that simulates transient infrastructure failures around the real ledger (L2-FR-08).
 * Keeps fault simulation out of business logic: production wiring simply doesn't use it.
 */
public final class FaultInjectingLedger implements Ledger {

    private final Ledger delegate;
    private final FaultPlan plan;

    public FaultInjectingLedger(Ledger delegate, FaultPlan plan) {
        this.delegate = delegate;
        this.plan = plan;
    }

    @Override
    public PostingResult post(LedgerEntry draft) {
        if (plan.shouldFailBeforeCommit(draft.transactionId())) {
            throw new TransientFailureException("simulated ledger timeout before commit");
        }
        PostingResult result = delegate.post(draft);
        if (plan.shouldFailAfterCommit(draft.transactionId())) {
            throw new TransientFailureException("simulated lost acknowledgement after commit");
        }
        return result;
    }

    @Override
    public Optional<LedgerEntry> findByTransactionId(String transactionId) {
        return delegate.findByTransactionId(transactionId);
    }

    @Override
    public BigDecimal balance(String accountId, String currency) {
        return delegate.balance(accountId, currency);
    }

    @Override
    public Map<String, BigDecimal> balances() {
        return delegate.balances();
    }

    @Override
    public List<LedgerEntry> entries() {
        return delegate.entries();
    }
}

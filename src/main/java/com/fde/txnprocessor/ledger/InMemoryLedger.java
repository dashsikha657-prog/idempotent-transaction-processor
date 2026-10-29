package com.fde.txnprocessor.ledger;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * In-memory ledger. One monitor guards entries and balances so every posting is a single
 * atomic unit (equivalent to one DB transaction with a UNIQUE(transaction_id) constraint).
 */
public final class InMemoryLedger implements Ledger {

    private final Clock clock;
    private final Map<String, LedgerEntry> byTransactionId = new HashMap<>();
    private final Map<String, String> reversedBy = new HashMap<>(); // original txn -> reversal txn
    private final Map<String, BigDecimal> balances = new HashMap<>();
    private final List<LedgerEntry> entries = new ArrayList<>();
    private long nextEntryNo = 1;

    public InMemoryLedger(Clock clock) {
        this.clock = clock;
    }

    public InMemoryLedger() {
        this(Clock.systemUTC());
    }

    @Override
    public synchronized PostingResult post(LedgerEntry draft) {
        LedgerEntry existing = byTransactionId.get(draft.transactionId());
        if (existing != null) {
            return new PostingResult(existing, true);
        }
        if (draft.reversalOf() != null && reversedBy.containsKey(draft.reversalOf())) {
            throw new LedgerConstraintException(LedgerConstraintException.Constraint.ALREADY_REVERSED,
                    "transaction already reversed");
        }
        String key = key(draft.accountId(), draft.currency());
        BigDecimal newBalance = balances.getOrDefault(key, BigDecimal.ZERO).add(draft.signedAmount());
        if (newBalance.signum() < 0) {
            throw new LedgerConstraintException(LedgerConstraintException.Constraint.INSUFFICIENT_FUNDS,
                    "posting would overdraw the account");
        }
        // ---- commit point: everything below happens together ----
        LedgerEntry committed = draft.committed(nextEntryNo++, clock.instant());
        byTransactionId.put(committed.transactionId(), committed);
        if (committed.reversalOf() != null) {
            reversedBy.put(committed.reversalOf(), committed.transactionId());
        }
        balances.put(key, newBalance);
        entries.add(committed);
        return new PostingResult(committed, false);
    }

    @Override
    public synchronized Optional<LedgerEntry> findByTransactionId(String transactionId) {
        return Optional.ofNullable(byTransactionId.get(transactionId));
    }

    @Override
    public synchronized BigDecimal balance(String accountId, String currency) {
        return balances.getOrDefault(key(accountId, currency), BigDecimal.ZERO);
    }

    @Override
    public synchronized Map<String, BigDecimal> balances() {
        return new TreeMap<>(balances);
    }

    @Override
    public synchronized List<LedgerEntry> entries() {
        return List.copyOf(entries);
    }

    private static String key(String accountId, String currency) {
        return accountId + ":" + currency;
    }
}

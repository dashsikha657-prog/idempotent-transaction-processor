package com.fde.txnprocessor.ledger;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The downstream side effect we must never duplicate: account balances + ledger entries.
 *
 * <p>Contract of {@link #post}:
 * <ul>
 *   <li><b>Atomic</b> — the entry and the balance change are applied together or not at all.</li>
 *   <li><b>Idempotent</b> — keyed by transactionId; posting the same transactionId again returns
 *       the existing entry with {@code alreadyApplied=true} and changes nothing.</li>
 *   <li><b>Guarded</b> — the balance may not go negative and a transaction may be reversed only once;
 *       these are checked inside the same atomic unit (no check-then-act race).</li>
 * </ul>
 * This is the second, independent line of defence: even if the state store were bypassed,
 * a duplicate cannot produce a second balance update.
 */
public interface Ledger {

    PostingResult post(LedgerEntry draft);

    Optional<LedgerEntry> findByTransactionId(String transactionId);

    BigDecimal balance(String accountId, String currency);

    /** accountId:currency -> balance. */
    Map<String, BigDecimal> balances();

    List<LedgerEntry> entries();
}

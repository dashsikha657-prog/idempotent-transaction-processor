package com.fde.txnprocessor.ledger;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * An immutable ledger posting. {@code signedAmount} is positive for credits, negative for debits.
 *
 * @param transactionId  idempotency key of the posting (UNIQUE in production)
 * @param reversalOf     transactionId this entry reverses, or null (UNIQUE in production — a
 *                       transaction can be reversed only once)
 */
public record LedgerEntry(
        long entryNo,
        String transactionId,
        String accountId,
        String currency,
        BigDecimal signedAmount,
        String reversalOf,
        long sequenceNumber,
        Instant postedAt) {

    /** A posting that has not been committed yet (entryNo/postedAt are assigned by the ledger). */
    public static LedgerEntry draft(String transactionId, String accountId, String currency,
                                    BigDecimal signedAmount, String reversalOf, long sequenceNumber) {
        return new LedgerEntry(0, transactionId, accountId, currency, signedAmount, reversalOf, sequenceNumber, null);
    }

    LedgerEntry committed(long entryNo, Instant at) {
        return new LedgerEntry(entryNo, transactionId, accountId, currency, signedAmount, reversalOf, sequenceNumber, at);
    }
}

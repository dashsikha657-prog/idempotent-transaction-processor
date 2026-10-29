package com.fde.txnprocessor.model;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

/**
 * A validated, immutable transaction request.
 *
 * @param transactionId          business transaction id — the idempotency key for the business effect
 * @param requestId              delivery/request id — the idempotency key for a specific delivery (client retries reuse it)
 * @param accountId              account reference
 * @param type                   CREDIT / DEBIT / REVERSAL
 * @param amount                 positive amount, max 2 decimal places
 * @param currency               ISO-4217 style 3-letter code
 * @param sequenceNumber         producer-assigned sequence used for ordering
 * @param eventTime              optional producer event time (informational only)
 * @param originalTransactionId  required for REVERSAL, otherwise null
 */
public record TransactionRequest(
        String transactionId,
        String requestId,
        String accountId,
        TransactionType type,
        BigDecimal amount,
        String currency,
        long sequenceNumber,
        Instant eventTime,
        String originalTransactionId) {

    public TransactionRequest {
        Objects.requireNonNull(transactionId, "transactionId");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(currency, "currency");
    }

    /**
     * Fingerprint of the business payload. Two requests for the same transactionId are a true
     * duplicate only if their fingerprints match; otherwise it is an idempotency conflict
     * (same key, different content), which must never be silently accepted or overwritten.
     *
     * <p>requestId and eventTime are deliberately excluded: a client retry may legitimately use a
     * new requestId or re-stamp the event time while describing the same business transaction.
     */
    public String fingerprint() {
        String canonical = String.join("|",
                transactionId,
                accountId,
                type.name(),
                amount.stripTrailingZeros().toPlainString(),
                currency,
                Long.toString(sequenceNumber),
                originalTransactionId == null ? "" : originalTransactionId);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}

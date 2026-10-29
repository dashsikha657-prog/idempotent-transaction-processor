package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.ledger.Ledger;
import com.fde.txnprocessor.ledger.LedgerConstraintException;
import com.fde.txnprocessor.ledger.LedgerEntry;
import com.fde.txnprocessor.ledger.PostingResult;
import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.TransactionRequest;

import java.math.BigDecimal;

/**
 * Pure business rules: turns a validated request into exactly one ledger posting.
 * Knows nothing about sources, ordering, retries or logging (L2-FR-10).
 */
public final class TransactionProcessor {

    private final Ledger ledger;

    public TransactionProcessor(Ledger ledger) {
        this.ledger = ledger;
    }

    /**
     * Applies the business effect of the request.
     *
     * @throws BusinessRuleException      deterministic rejection (do not retry)
     * @throws TransientFailureException  infrastructure hiccup (safe to retry: the posting is idempotent)
     */
    public PostingResult apply(TransactionRequest request) {
        LedgerEntry draft = switch (request.type()) {
            case CREDIT -> posting(request, request.amount(), null);
            case DEBIT -> posting(request, request.amount().negate(), null);
            case REVERSAL -> reversal(request);
        };
        try {
            return ledger.post(draft);
        } catch (LedgerConstraintException e) {
            FailureReason reason = switch (e.constraint()) {
                case INSUFFICIENT_FUNDS -> FailureReason.INSUFFICIENT_FUNDS;
                case ALREADY_REVERSED -> FailureReason.INVALID_REVERSAL;
            };
            throw new BusinessRuleException(reason, e.getMessage());
        }
    }

    private LedgerEntry reversal(TransactionRequest request) {
        // If this reversal was already posted (retry after lost ack), let the ledger's
        // idempotency return the existing entry instead of re-checking the original.
        if (ledger.findByTransactionId(request.transactionId()).isPresent()) {
            LedgerEntry existing = ledger.findByTransactionId(request.transactionId()).get();
            return posting(request, existing.signedAmount(), existing.reversalOf());
        }
        LedgerEntry original = ledger.findByTransactionId(request.originalTransactionId())
                .orElseThrow(() -> new BusinessRuleException(FailureReason.INVALID_REVERSAL,
                        "original transaction has not been posted"));
        if (original.reversalOf() != null) {
            throw new BusinessRuleException(FailureReason.INVALID_REVERSAL, "cannot reverse a reversal");
        }
        if (!original.accountId().equals(request.accountId()) || !original.currency().equals(request.currency())) {
            throw new BusinessRuleException(FailureReason.INVALID_REVERSAL,
                    "reversal account/currency does not match the original");
        }
        if (original.signedAmount().abs().compareTo(request.amount()) != 0) {
            throw new BusinessRuleException(FailureReason.INVALID_REVERSAL,
                    "only full reversals are supported; amount must equal the original");
        }
        return posting(request, original.signedAmount().negate(), original.transactionId());
    }

    private static LedgerEntry posting(TransactionRequest r, BigDecimal signedAmount, String reversalOf) {
        return LedgerEntry.draft(r.transactionId(), r.accountId(), r.currency(), signedAmount, reversalOf,
                r.sequenceNumber());
    }
}

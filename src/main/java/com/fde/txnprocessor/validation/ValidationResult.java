package com.fde.txnprocessor.validation;

import com.fde.txnprocessor.model.TransactionRequest;

import java.util.List;
import java.util.OptionalLong;

/**
 * Outcome of validating a raw record.
 *
 * @param request             the validated request, or null if invalid
 * @param errors              validation errors (never contain raw input values)
 * @param rawRequestId        best-effort requestId for reporting (only if it passed the id format check)
 * @param rawTransactionId    best-effort transactionId for reporting (only if it passed the id format check)
 * @param rawAccountId        best-effort accountId (only if well-formed), used to locate the ordering partition
 * @param bestEffortSequence  the sequence number if it parsed, even when other fields are invalid
 */
public record ValidationResult(
        TransactionRequest request,
        List<String> errors,
        String rawRequestId,
        String rawTransactionId,
        String rawAccountId,
        OptionalLong bestEffortSequence) {

    public boolean isValid() {
        return request != null && errors.isEmpty();
    }
}

package com.fde.txnprocessor.validation;

import com.fde.txnprocessor.ingest.RawRecord;
import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Pattern;

/**
 * Validates raw records before any state is touched (L2-FR-02).
 *
 * <p>Security notes: identifiers are restricted to a safe character set (prevents log
 * injection and odd keys in the state store), and error messages never echo raw values
 * back, so malformed or sensitive input does not leak into logs or reports.
 */
public final class RequestValidator {

    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");

    private final BigDecimal maxAmount;

    public RequestValidator(BigDecimal maxAmount) {
        this.maxAmount = maxAmount;
    }

    public RequestValidator() {
        this(new BigDecimal("1000000000"));
    }

    public ValidationResult validate(RawRecord raw) {
        List<String> errors = new ArrayList<>();

        String transactionId = raw.get(RawRecord.TRANSACTION_ID);
        String requestId = raw.get(RawRecord.REQUEST_ID);
        String accountId = raw.get(RawRecord.ACCOUNT_ID);
        String goodTxnId = ID.matcher(nullToEmpty(transactionId)).matches() ? transactionId : null;
        String goodReqId = ID.matcher(nullToEmpty(requestId)).matches() ? requestId : null;
        String goodAccount = ID.matcher(nullToEmpty(accountId)).matches() ? accountId : null;

        if (raw.parseError() != null) {
            errors.add("unparseable record: " + raw.parseError());
            return new ValidationResult(null, errors, null, null, null, OptionalLong.empty());
        }

        checkId(RawRecord.TRANSACTION_ID, transactionId, errors);
        checkId(RawRecord.REQUEST_ID, requestId, errors);
        checkId(RawRecord.ACCOUNT_ID, accountId, errors);

        TransactionType type = parseType(raw.get(RawRecord.TRANSACTION_TYPE), errors);
        BigDecimal amount = parseAmount(raw.get(RawRecord.AMOUNT), errors);
        String currency = raw.get(RawRecord.CURRENCY);
        if (currency == null) {
            errors.add("currency is required");
        } else if (!CURRENCY.matcher(currency).matches()) {
            errors.add("currency must be a 3-letter upper-case code");
        }
        OptionalLong sequence = parseSequence(raw.get(RawRecord.SEQUENCE_NUMBER), errors);
        Instant eventTime = parseEventTime(raw.get(RawRecord.EVENT_TIME), errors);

        String originalTxnId = raw.get(RawRecord.ORIGINAL_TRANSACTION_ID);
        if (type == TransactionType.REVERSAL) {
            if (originalTxnId == null) {
                errors.add("originalTransactionId is required for REVERSAL");
            } else if (!ID.matcher(originalTxnId).matches()) {
                errors.add("originalTransactionId has an invalid format");
            } else if (originalTxnId.equals(transactionId)) {
                errors.add("a REVERSAL cannot reverse itself");
            }
        } else if (originalTxnId != null) {
            errors.add("originalTransactionId is only allowed for REVERSAL");
        }

        if (!errors.isEmpty()) {
            return new ValidationResult(null, List.copyOf(errors), goodReqId, goodTxnId, goodAccount, sequence);
        }
        TransactionRequest request = new TransactionRequest(transactionId, requestId, accountId, type,
                amount, currency, sequence.getAsLong(), eventTime, originalTxnId);
        return new ValidationResult(request, List.of(), requestId, transactionId, accountId, sequence);
    }

    private static void checkId(String field, String value, List<String> errors) {
        if (value == null) {
            errors.add(field + " is required");
        } else if (!ID.matcher(value).matches()) {
            errors.add(field + " has an invalid format (allowed: letters, digits, '-', '_', max 64)");
        }
    }

    private static TransactionType parseType(String value, List<String> errors) {
        if (value == null) {
            errors.add("transactionType is required");
            return null;
        }
        try {
            return TransactionType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            errors.add("transactionType must be one of CREDIT, DEBIT, REVERSAL");
            return null;
        }
    }

    private BigDecimal parseAmount(String value, List<String> errors) {
        if (value == null) {
            errors.add("amount is required");
            return null;
        }
        BigDecimal amount;
        try {
            amount = new BigDecimal(value);
        } catch (NumberFormatException e) {
            errors.add("amount must be a decimal number");
            return null;
        }
        if (amount.signum() <= 0) {
            errors.add("amount must be greater than zero");
        }
        if (amount.stripTrailingZeros().scale() > 2) {
            errors.add("amount must have at most 2 decimal places");
        }
        if (amount.compareTo(maxAmount) > 0) {
            errors.add("amount exceeds the configured maximum");
        }
        return amount;
    }

    private static OptionalLong parseSequence(String value, List<String> errors) {
        if (value == null) {
            errors.add("sequenceNumber is required");
            return OptionalLong.empty();
        }
        try {
            long seq = Long.parseLong(value);
            if (seq <= 0) {
                errors.add("sequenceNumber must be a positive integer");
                return OptionalLong.empty();
            }
            return OptionalLong.of(seq);
        } catch (NumberFormatException e) {
            errors.add("sequenceNumber must be a positive integer");
            return OptionalLong.empty();
        }
    }

    private static Instant parseEventTime(String value, List<String> errors) {
        if (value == null) {
            return null; // optional
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            errors.add("eventTime must be an ISO-8601 instant, e.g. 2026-10-29T10:15:30Z");
            return null;
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}

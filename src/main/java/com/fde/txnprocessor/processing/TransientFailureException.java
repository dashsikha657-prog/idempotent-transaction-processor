package com.fde.txnprocessor.processing;

/**
 * A failure that may succeed if retried (timeout, deadlock, connection reset, 503...).
 * Only this exception type triggers the retry policy; everything else fails fast.
 */
public class TransientFailureException extends RuntimeException {
    public TransientFailureException(String message) {
        super(message);
    }
}

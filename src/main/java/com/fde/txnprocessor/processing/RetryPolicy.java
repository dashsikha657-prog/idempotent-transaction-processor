package com.fde.txnprocessor.processing;

import java.time.Duration;

/**
 * Bounded exponential backoff.
 *
 * @param maxAttempts total attempts including the first (3 = first try + 2 retries)
 * @param baseDelay   delay before the first retry
 * @param maxDelay    cap on the delay
 */
public record RetryPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5));
    }

    public boolean canRetryAfter(int attemptsSoFar) {
        return attemptsSoFar < maxAttempts;
    }

    /** Delay before the next attempt, after {@code attemptsSoFar} failed attempts: base * 2^(n-1). */
    public Duration delayAfter(int attemptsSoFar) {
        long factor = 1L << Math.min(20, Math.max(0, attemptsSoFar - 1));
        Duration d = baseDelay.multipliedBy(factor);
        return d.compareTo(maxDelay) > 0 ? maxDelay : d;
    }
}

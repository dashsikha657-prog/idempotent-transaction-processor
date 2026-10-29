package com.fde.txnprocessor.processing;

import java.time.Duration;

/** Abstraction over waiting so tests run instantly and deterministically. */
@FunctionalInterface
public interface Sleeper {

    void sleep(Duration duration);

    Sleeper NO_OP = d -> { };

    Sleeper REAL = d -> {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    };
}

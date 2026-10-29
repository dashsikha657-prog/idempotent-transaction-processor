package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.model.TransactionRequest;

/**
 * Scope of the sequence number.
 *
 * <ul>
 *   <li>GLOBAL — one sequence across the whole stream (matches the assessment test data,
 *       where sequences 1..5 span several accounts).</li>
 *   <li>PER_ACCOUNT — each account has its own sequence starting at 1 (typical of
 *       Kafka-partitioned-by-account designs; gives parallelism across accounts).</li>
 * </ul>
 */
public enum OrderingMode {
    GLOBAL,
    PER_ACCOUNT;

    static final String GLOBAL_PARTITION = "*";

    public String partitionOf(TransactionRequest request) {
        return partitionOf(request.accountId());
    }

    /** @return the partition, or null when it cannot be determined (PER_ACCOUNT with unknown account). */
    public String partitionOf(String accountId) {
        return this == GLOBAL ? GLOBAL_PARTITION : accountId;
    }
}

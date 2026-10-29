package com.fde.txnprocessor.model;

/** Business transaction types supported by the service. */
public enum TransactionType {
    /** Adds the amount to the account balance. */
    CREDIT,
    /** Subtracts the amount from the account balance (must not overdraw). */
    DEBIT,
    /** Fully reverses a previously posted transaction (identified by originalTransactionId). */
    REVERSAL
}

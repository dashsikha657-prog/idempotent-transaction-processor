package com.fde.txnprocessor.observability;

/** Masks customer identifiers before they reach logs or reports. */
public final class Masking {

    private Masking() { }

    /** "ACC-201" -> "****201". Keeps the last 3 characters so operators can still correlate. */
    public static String account(String accountId) {
        if (accountId == null || accountId.isEmpty()) {
            return "-";
        }
        int keep = Math.min(3, accountId.length());
        return "*".repeat(Math.max(4, accountId.length() - keep)) + accountId.substring(accountId.length() - keep);
    }

    /** "ACC-201:INR" -> "****201:INR". */
    public static String balanceKey(String key) {
        int idx = key.lastIndexOf(':');
        return idx < 0 ? account(key) : account(key.substring(0, idx)) + key.substring(idx);
    }
}

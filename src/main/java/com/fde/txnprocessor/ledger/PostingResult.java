package com.fde.txnprocessor.ledger;

/**
 * @param entry            the committed entry (new or pre-existing)
 * @param alreadyApplied   true if the posting already existed — an idempotent no-op, e.g. a retry
 *                         after the first attempt committed but its acknowledgement was lost
 */
public record PostingResult(LedgerEntry entry, boolean alreadyApplied) { }

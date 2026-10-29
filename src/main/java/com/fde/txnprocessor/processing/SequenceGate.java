package com.fde.txnprocessor.processing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Out-of-order handling — "Option A: sequence-based processing" (L2-FR-07).
 *
 * <p>Per partition the gate tracks the next expected sequence number. A request is released
 * for processing only when it is the next one; future sequences are parked (status PENDING)
 * and released automatically when the gap closes. A sequence slot is <i>closed</i> when its
 * request reaches a terminal outcome (PROCESSED or FAILED), so one poison record cannot block
 * the stream forever. Records that fail validation but carry a readable sequence close their
 * slot with a tombstone for the same reason.
 *
 * <p>A request in RETRY_PENDING keeps its slot: later sequences wait. That is deliberate —
 * it preserves ordering (e.g. a credit that is retrying must land before the debit that relies
 * on it) at the cost of head-of-line blocking for the retry budget duration.
 */
public final class SequenceGate {

    public enum Admission {
        /** It's this request's turn — process now. */
        READY,
        /** An earlier sequence is still missing — park it. */
        PARKED,
        /** Sequence already consumed. */
        STALE,
        /** Another request already holds or waits on this sequence. */
        CONFLICT
    }

    /** Gap information for the processing report. */
    public record Gap(String partition, long nextExpected, List<Long> parked, List<Long> missing) { }

    private static final String TOMBSTONE = "\u0000tombstone";

    private static final class PartitionState {
        long nextExpected;
        final TreeMap<Long, String> waiting = new TreeMap<>(); // seq -> requestId or TOMBSTONE
        final Set<Long> inFlight = new HashSet<>();

        PartitionState(long first) {
            this.nextExpected = first;
        }
    }

    private final long firstSequence;
    private final Map<String, PartitionState> partitions = new HashMap<>();

    public SequenceGate(long firstSequence) {
        this.firstSequence = firstSequence;
    }

    public SequenceGate() {
        this(1);
    }

    public synchronized Admission admit(String partition, long seq, String requestId) {
        PartitionState p = state(partition);
        if (seq < p.nextExpected) {
            return Admission.STALE;
        }
        if (p.inFlight.contains(seq) || p.waiting.containsKey(seq)) {
            return Admission.CONFLICT;
        }
        if (seq == p.nextExpected) {
            p.inFlight.add(seq);
            return Admission.READY;
        }
        p.waiting.put(seq, requestId);
        return Admission.PARKED;
    }

    /**
     * Marks the in-flight sequence as finished (terminal outcome) and returns the requestId that
     * is now allowed to run, if any.
     */
    public synchronized Optional<String> complete(String partition, long seq) {
        PartitionState p = state(partition);
        p.inFlight.remove(seq);
        if (seq != p.nextExpected) {
            return Optional.empty(); // e.g. an operator redrive of a slot that was already closed
        }
        p.nextExpected++;
        return advance(p);
    }

    /**
     * Closes a slot for a record that will never be processed (failed validation). If the slot is
     * the next expected one, the stream advances immediately; otherwise a tombstone is left so the
     * stream skips it when it gets there.
     */
    public synchronized Optional<String> closeWithoutProcessing(String partition, long seq) {
        PartitionState p = state(partition);
        if (seq < p.nextExpected || p.inFlight.contains(seq) || p.waiting.containsKey(seq)) {
            return Optional.empty(); // slot legitimately owned by someone else, or already closed
        }
        if (seq == p.nextExpected) {
            p.nextExpected++;
            return advance(p);
        }
        p.waiting.put(seq, TOMBSTONE);
        return Optional.empty();
    }

    private Optional<String> advance(PartitionState p) {
        while (true) {
            String next = p.waiting.get(p.nextExpected);
            if (next == null) {
                return Optional.empty();
            }
            p.waiting.remove(p.nextExpected);
            if (TOMBSTONE.equals(next)) {
                p.nextExpected++;
                continue;
            }
            p.inFlight.add(p.nextExpected);
            return Optional.of(next);
        }
    }

    /** Partitions that still have parked requests, with the sequence numbers that never arrived. */
    public synchronized List<Gap> gaps() {
        List<Gap> gaps = new ArrayList<>();
        for (Map.Entry<String, PartitionState> e : new TreeMap<>(partitions).entrySet()) {
            PartitionState p = e.getValue();
            List<Long> parked = new ArrayList<>();
            p.waiting.forEach((seq, id) -> { if (!TOMBSTONE.equals(id)) parked.add(seq); });
            if (parked.isEmpty()) {
                continue;
            }
            List<Long> missing = new ArrayList<>();
            long max = parked.get(parked.size() - 1);
            for (long s = p.nextExpected; s < max; s++) {
                if (!p.waiting.containsKey(s) && !p.inFlight.contains(s)) {
                    missing.add(s);
                }
            }
            gaps.add(new Gap(e.getKey(), p.nextExpected, parked, missing));
        }
        return gaps;
    }

    private PartitionState state(String partition) {
        return partitions.computeIfAbsent(partition, k -> new PartitionState(firstSequence));
    }
}

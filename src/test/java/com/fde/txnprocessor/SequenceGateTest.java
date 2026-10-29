package com.fde.txnprocessor;

import com.fde.txnprocessor.processing.SequenceGate;
import com.fde.txnprocessor.processing.SequenceGate.Admission;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequenceGateTest {

    @Test
    @DisplayName("Admission rules: READY / PARKED / STALE / CONFLICT")
    void admissionRules() {
        SequenceGate g = new SequenceGate();
        assertEquals(Admission.PARKED, g.admit("p", 3, "R3"));
        assertEquals(Admission.CONFLICT, g.admit("p", 3, "R3-other"));
        assertEquals(Admission.READY, g.admit("p", 1, "R1"));
        assertEquals(Admission.CONFLICT, g.admit("p", 1, "R1-other")); // in flight
        assertEquals(Optional.empty(), g.complete("p", 1));             // 2 missing -> nothing released
        assertEquals(Admission.STALE, g.admit("p", 1, "R1-late"));
        assertEquals(Admission.READY, g.admit("p", 2, "R2"));
        assertEquals(Optional.of("R3"), g.complete("p", 2));           // 3 released
        assertEquals(Optional.empty(), g.complete("p", 3));
        assertTrue(g.gaps().isEmpty());
    }

    @Test
    @DisplayName("Tombstones are skipped when the stream reaches them")
    void tombstonesAreSkipped() {
        SequenceGate g = new SequenceGate();
        g.admit("p", 4, "R4");
        g.closeWithoutProcessing("p", 2);
        g.closeWithoutProcessing("p", 3);
        assertEquals(Admission.READY, g.admit("p", 1, "R1"));
        assertEquals(Optional.of("R4"), g.complete("p", 1));
    }

    @Test
    @DisplayName("Gaps list parked and missing sequences per partition")
    void gapReporting() {
        SequenceGate g = new SequenceGate();
        g.admit("a", 3, "A3");
        g.admit("a", 6, "A6");
        g.closeWithoutProcessing("a", 5);
        g.admit("b", 1, "B1");

        List<SequenceGate.Gap> gaps = g.gaps();
        assertEquals(1, gaps.size());
        assertEquals("a", gaps.get(0).partition());
        assertEquals(List.of(3L, 6L), gaps.get(0).parked());
        assertEquals(List.of(1L, 2L, 4L), gaps.get(0).missing());
    }
}

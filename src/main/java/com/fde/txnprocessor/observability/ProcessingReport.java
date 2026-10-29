package com.fde.txnprocessor.observability;

import com.fde.txnprocessor.model.ProcessingRecord;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.processing.SequenceGate;

import java.math.BigDecimal;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** End-of-run processing summary (L2-FR-11): counts by status, per-record lifecycle, gaps, balances. */
public record ProcessingReport(
        String source,
        Map<TransactionStatus, Long> statusCounts,
        List<RecordLine> records,
        List<SequenceGate.Gap> gaps,
        Map<String, BigDecimal> maskedBalances,
        int ledgerEntries,
        Map<String, Long> metrics) {

    /** One line per request. No amounts, masked account. */
    public record RecordLine(String requestId, String transactionId, String sourceRef, TransactionStatus status,
                             int attempts, String failureReason, String detail, String lifecycle) { }

    public static ProcessingReport build(String source, List<ProcessingRecord> all, List<SequenceGate.Gap> gaps,
                                         Map<String, BigDecimal> balances, int ledgerEntries,
                                         ProcessingMetrics metrics) {
        Map<TransactionStatus, Long> counts = new EnumMap<>(TransactionStatus.class);
        for (TransactionStatus s : TransactionStatus.values()) {
            counts.put(s, 0L);
        }
        all.forEach(r -> counts.merge(r.status(), 1L, Long::sum));

        List<RecordLine> lines = all.stream().map(r -> new RecordLine(
                nz(r.requestId()), nz(r.transactionId()), r.sourceRef(), r.status(), r.attempts(),
                r.failureReason() == null ? "-" : r.failureReason().name(),
                r.detail() == null ? "" : r.detail(),
                r.lifecycle())).toList();

        Map<String, BigDecimal> masked = new LinkedHashMap<>();
        balances.forEach((k, v) -> masked.put(Masking.balanceKey(k), v));

        return new ProcessingReport(source, counts, lines, gaps, masked, ledgerEntries, metrics.snapshot());
    }

    public long count(TransactionStatus status) {
        return statusCounts.getOrDefault(status, 0L);
    }

    public String toText() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n================ PROCESSING REPORT ================\n");
        sb.append("Source: ").append(source).append("\n\n");

        sb.append("Counts by final status:\n");
        statusCounts.forEach((s, c) -> sb.append(String.format("  %-14s %d%n", s, c)));

        sb.append("\nRecords (arrival order):\n");
        sb.append(String.format("  %-11s %-10s %-13s %-3s %-20s %s%n",
                "requestId", "txnId", "status", "try", "failureReason", "lifecycle / detail"));
        for (RecordLine l : records) {
            sb.append(String.format("  %-11s %-10s %-13s %-3d %-20s %s%n",
                    l.requestId(), l.transactionId(), l.status(), l.attempts(), l.failureReason(), l.lifecycle()));
            if (!l.detail().isEmpty()) {
                sb.append(String.format("  %-60s   detail: %s%n", "", l.detail()));
            }
        }

        sb.append("\nOrdering gaps (parked requests still waiting):\n");
        if (gaps.isEmpty()) {
            sb.append("  none\n");
        } else {
            for (SequenceGate.Gap g : gaps) {
                sb.append("  partition=").append(g.partition())
                        .append(" nextExpected=").append(g.nextExpected())
                        .append(" parked=").append(g.parked())
                        .append(" missing=").append(g.missing()).append('\n');
            }
        }

        sb.append("\nLedger: ").append(ledgerEntries).append(" entries; balances (masked):\n");
        maskedBalances.forEach((k, v) -> sb.append(String.format("  %-16s %s%n", k, v.toPlainString())));

        sb.append("\nMetrics:\n");
        metrics.forEach((k, v) -> sb.append(String.format("  %-22s %d%n", k, v)));
        sb.append("===================================================\n");
        return sb.toString();
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder("{\n");
        sb.append("  \"source\": ").append(q(source)).append(",\n");
        sb.append("  \"statusCounts\": {")
                .append(statusCounts.entrySet().stream().map(e -> q(e.getKey().name()) + ": " + e.getValue())
                        .collect(Collectors.joining(", ")))
                .append("},\n");
        sb.append("  \"records\": [\n");
        sb.append(records.stream().map(l -> "    {" + String.join(", ",
                q("requestId") + ": " + q(l.requestId()),
                q("transactionId") + ": " + q(l.transactionId()),
                q("sourceRef") + ": " + q(l.sourceRef()),
                q("status") + ": " + q(l.status().name()),
                q("attempts") + ": " + l.attempts(),
                q("failureReason") + ": " + q(l.failureReason()),
                q("detail") + ": " + q(l.detail()),
                q("lifecycle") + ": " + q(l.lifecycle())) + "}").collect(Collectors.joining(",\n")));
        sb.append("\n  ],\n");
        sb.append("  \"gaps\": [").append(gaps.stream().map(g -> "{" + q("partition") + ": " + q(g.partition())
                + ", " + q("nextExpected") + ": " + g.nextExpected()
                + ", " + q("parked") + ": " + g.parked()
                + ", " + q("missing") + ": " + g.missing() + "}").collect(Collectors.joining(", "))).append("],\n");
        sb.append("  \"ledgerEntries\": ").append(ledgerEntries).append(",\n");
        sb.append("  \"balances\": {").append(maskedBalances.entrySet().stream()
                .map(e -> q(e.getKey()) + ": " + e.getValue().toPlainString()).collect(Collectors.joining(", ")))
                .append("},\n");
        sb.append("  \"metrics\": {").append(metrics.entrySet().stream()
                .map(e -> q(e.getKey()) + ": " + e.getValue()).collect(Collectors.joining(", "))).append("}\n");
        sb.append("}\n");
        return sb.toString();
    }

    private static String nz(String s) {
        return s == null ? "-" : s;
    }

    private static String q(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}

package com.fde.txnprocessor;

import com.fde.txnprocessor.ingest.CsvTransactionSource;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.processing.EngineBuilder;
import com.fde.txnprocessor.processing.OrderingMode;
import com.fde.txnprocessor.processing.ProcessingEngine;
import com.fde.txnprocessor.processing.RetryPolicy;
import com.fde.txnprocessor.simulation.FaultPlan;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Command-line entry point: runs one background batch over a CSV file and prints the report.
 *
 * <pre>
 * java -jar target/txn-processor-1.0.0.jar [input.csv] [options]
 *   --fail-before-commit TXN-ID:N   simulate N transient failures before the ledger commit
 *   --fail-after-commit  TXN-ID:N   simulate N lost acknowledgements after the ledger commit
 *   --max-attempts N                retry budget (default 3)
 *   --base-delay-ms N               first backoff delay (default 100)
 *   --ordering GLOBAL|PER_ACCOUNT   sequence scope (default GLOBAL)
 *   --report-json FILE              also write the report as JSON
 * </pre>
 * Logs (structured, key=value) go to stderr; the report goes to stdout.
 */
public final class App {

    public static void main(String[] args) throws IOException {
        Path input = Path.of("data/sample-transactions.csv");
        FaultPlan faults = FaultPlan.none();
        int maxAttempts = 3;
        long baseDelayMs = 100;
        OrderingMode ordering = OrderingMode.GLOBAL;
        Path jsonOut = null;

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--fail-before-commit" -> { String[] f = pair(args, ++i, a); faults.failBeforeCommit(f[0], Integer.parseInt(f[1])); }
                case "--fail-after-commit" -> { String[] f = pair(args, ++i, a); faults.failAfterCommit(f[0], Integer.parseInt(f[1])); }
                case "--max-attempts" -> maxAttempts = Integer.parseInt(value(args, ++i, a));
                case "--base-delay-ms" -> baseDelayMs = Long.parseLong(value(args, ++i, a));
                case "--ordering" -> ordering = OrderingMode.valueOf(value(args, ++i, a).toUpperCase());
                case "--report-json" -> jsonOut = Path.of(value(args, ++i, a));
                case "-h", "--help" -> { usage(); return; }
                default -> {
                    if (a.startsWith("--")) {
                        System.err.println("Unknown option: " + a);
                        usage();
                        System.exit(64);
                    }
                    input = Path.of(a);
                }
            }
        }
        if (!Files.isReadable(input)) {
            System.err.println("Input file not found or not readable: " + input);
            System.exit(66);
        }

        ProcessingEngine engine = new EngineBuilder()
                .faultPlan(faults)
                .orderingMode(ordering)
                .retryPolicy(new RetryPolicy(maxAttempts, Duration.ofMillis(baseDelayMs), Duration.ofSeconds(5)))
                .build();

        ProcessingReport report = engine.run(new CsvTransactionSource(input));
        System.out.print(report.toText());
        if (jsonOut != null) {
            Files.writeString(jsonOut, report.toJson(), StandardCharsets.UTF_8);
            System.out.println("JSON report written to " + jsonOut);
        }
    }

    private static String value(String[] args, int i, String opt) {
        if (i >= args.length) {
            throw new IllegalArgumentException(opt + " requires a value");
        }
        return args[i];
    }

    private static String[] pair(String[] args, int i, String opt) {
        String[] parts = value(args, i, opt).split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException(opt + " expects TXN-ID:COUNT");
        }
        return parts;
    }

    private static void usage() {
        System.err.println("""
                Usage: java -jar target/txn-processor-1.0.0.jar [input.csv] [options]
                  --fail-before-commit TXN-ID:N   simulate N transient failures before the ledger commit
                  --fail-after-commit  TXN-ID:N   simulate N lost acknowledgements after the ledger commit
                  --max-attempts N                retry budget incl. first attempt (default 3)
                  --base-delay-ms N               first backoff delay in ms (default 100)
                  --ordering GLOBAL|PER_ACCOUNT   sequence scope (default GLOBAL)
                  --report-json FILE              also write the report as JSON""");
    }
}

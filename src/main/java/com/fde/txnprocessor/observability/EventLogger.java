package com.fde.txnprocessor.observability;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Minimal structured (logfmt-style key=value) event logger.
 *
 * <p>Every processing decision emits one event with a stable {@code event} name, so log
 * pipelines (ELK, Loki, CloudWatch Insights) can count PROCESSED / DUPLICATE / RETRY_SCHEDULED /
 * FAILED without parsing free text. Amounts are never logged and account ids are masked.
 * A real service would emit the same fields through SLF4J + a JSON encoder.
 */
public final class EventLogger {

    public enum Level { INFO, WARN, ERROR }

    /** A captured event (kept in memory so tests can assert on observability). */
    public record Event(Level level, String name, Map<String, String> fields, String line) { }

    private final Clock clock;
    private final Consumer<String> sink;
    private final List<Event> events = Collections.synchronizedList(new ArrayList<>());

    public EventLogger(Clock clock, Consumer<String> sink) {
        this.clock = clock;
        this.sink = sink;
    }

    public static EventLogger toStderr(Clock clock) {
        return new EventLogger(clock, System.err::println);
    }

    public static EventLogger silent(Clock clock) {
        return new EventLogger(clock, line -> { });
    }

    public void info(String event, String... kv) { log(Level.INFO, event, kv); }
    public void warn(String event, String... kv) { log(Level.WARN, event, kv); }
    public void error(String event, String... kv) { log(Level.ERROR, event, kv); }

    private void log(Level level, String event, String... kv) {
        if (kv.length % 2 != 0) {
            throw new IllegalArgumentException("key/value pairs expected");
        }
        Map<String, String> fields = new LinkedHashMap<>();
        StringBuilder sb = new StringBuilder()
                .append("ts=").append(clock.instant())
                .append(" level=").append(level)
                .append(" event=").append(event);
        for (int i = 0; i < kv.length; i += 2) {
            String value = kv[i + 1] == null ? "-" : kv[i + 1];
            fields.put(kv[i], value);
            sb.append(' ').append(kv[i]).append('=').append(quoteIfNeeded(value));
        }
        String line = sb.toString();
        events.add(new Event(level, event, fields, line));
        sink.accept(line);
    }

    private static String quoteIfNeeded(String v) {
        String safe = v.replace("\n", " ").replace("\r", " ");
        return safe.contains(" ") || safe.contains("=") ? "\"" + safe.replace("\"", "'") + "\"" : safe;
    }

    public List<Event> events() {
        synchronized (events) {
            return List.copyOf(events);
        }
    }
}

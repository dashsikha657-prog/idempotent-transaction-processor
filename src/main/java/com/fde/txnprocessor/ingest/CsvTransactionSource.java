package com.fde.txnprocessor.ingest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Reads transaction requests from a header-based CSV file.
 *
 * <p>Format: first non-comment line is the header; blank lines and lines starting with '#'
 * are ignored. Values must not contain commas (no quoting support — sufficient for this
 * exercise; a real file adapter would use a proper CSV library).
 *
 * <p>A row with the wrong number of columns does not abort the file: it is emitted as an
 * unparseable {@link RawRecord} so the engine can reject and report it in isolation.
 */
public final class CsvTransactionSource implements TransactionSource {

    private final Path path;

    public CsvTransactionSource(Path path) {
        this.path = path;
    }

    @Override
    public Stream<RawRecord> read() {
        List<String> lines;
        try {
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read input file " + path.getFileName(), e);
        }

        List<RawRecord> out = new ArrayList<>();
        String[] header = null;
        String fileName = path.getFileName().toString();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank() || line.stripLeading().startsWith("#")) {
                continue;
            }
            String[] cols = line.split(",", -1);
            if (header == null) {
                header = Arrays.stream(cols).map(String::trim).toArray(String[]::new);
                continue;
            }
            String ref = fileName + ":line " + (i + 1);
            if (cols.length != header.length) {
                out.add(RawRecord.unparseable(ref,
                        "expected " + header.length + " columns but found " + cols.length));
                continue;
            }
            Map<String, String> fields = new LinkedHashMap<>();
            for (int c = 0; c < header.length; c++) {
                String v = cols[c].trim();
                fields.put(header[c], v.isEmpty() ? null : v);
            }
            out.add(RawRecord.of(ref, fields));
        }
        return out.stream();
    }

    @Override
    public String name() {
        return "csv(" + path.getFileName() + ")";
    }
}

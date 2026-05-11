package com.wualabs.qtsurfer.lastra;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;

/**
 * Generates .lastra fixture files for cross-language testing (Java writer → TS reader).
 * Run with: mvn exec:java -Dexec.mainClass=...LastraTestFixtureGenerator -Dexec.args="/output/dir"
 */
public class LastraTestFixtureGenerator {

    public static void main(String[] args) throws Exception {
        Path outDir = args.length > 0 ? Path.of(args[0]) : Path.of("target/fixtures");
        Files.createDirectories(outDir);

        generateSeriesOnly(outDir);
        generateWithMetadata(outDir);
        generateWithEvents(outDir);
        generateWithRowGroups(outDir);
        generateMultipleWriteSeriesCalls(outDir);

        System.out.println("Fixtures written to " + outDir);
    }

    private static void generateSeriesOnly(Path dir) throws Exception {
        int rows = 100;
        long[] ts = new long[rows];
        double[] close = new double[rows];
        Random rng = new Random(42);
        long baseTs = 1711152000000000000L;
        for (int i = 0; i < rows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            close[i] = Math.round((65000.0 + Math.sin(i * 0.01) * 500 + rng.nextDouble() * 10) * 100.0) / 100.0;
        }

        try (LastraWriter w = new LastraWriter(new FileOutputStream(dir.resolve("series-only.lastra").toFile()))) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(rows, ts, close);
        }
    }

    private static void generateWithMetadata(Path dir) throws Exception {
        int rows = 50;
        long[] ts = new long[rows];
        double[] ema = new double[rows];
        for (int i = 0; i < rows; i++) {
            ts[i] = 1711152000000000000L + i * 1_000_000_000L;
            ema[i] = Math.round((65000.0 + i * 0.1) * 100.0) / 100.0;
        }

        try (LastraWriter w = new LastraWriter(new FileOutputStream(dir.resolve("with-metadata.lastra").toFile()))) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("ema1", Lastra.DataType.DOUBLE, Lastra.Codec.ALP,
                    Map.of("indicator", "ema", "periods", "10"));
            w.writeSeries(rows, ts, ema);
        }
    }

    private static void generateWithEvents(Path dir) throws Exception {
        int rows = 50;
        long[] ts = new long[rows];
        double[] close = new double[rows];
        Random rng = new Random(42);
        long baseTs = 1711152000000000000L;
        for (int i = 0; i < rows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            close[i] = Math.round((65000.0 + rng.nextDouble() * 100) * 100.0) / 100.0;
        }

        int eventCount = 3;
        long[] eventTs = {baseTs + 10_000_000_000L, baseTs + 25_000_000_000L, baseTs + 40_000_000_000L};
        byte[][] types = {"BUY".getBytes(StandardCharsets.UTF_8), "SELL".getBytes(StandardCharsets.UTF_8), "STOP_LOSS".getBytes(StandardCharsets.UTF_8)};
        byte[][] data = {"{\"price\":65042.17}".getBytes(StandardCharsets.UTF_8), null, "{\"reason\":\"stop\"}".getBytes(StandardCharsets.UTF_8)};

        try (LastraWriter w = new LastraWriter(new FileOutputStream(dir.resolve("with-events.lastra").toFile()))) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addEventColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addEventColumn("type", Lastra.DataType.BINARY, Lastra.Codec.VARLEN);
            w.addEventColumn("data", Lastra.DataType.BINARY, Lastra.Codec.VARLEN_ZSTD);
            w.writeSeries(rows, ts, close);
            w.writeEvents(eventCount, eventTs, types, data);
        }
    }

    private static void generateWithRowGroups(Path dir) throws Exception {
        int totalRows = 10000;
        int rgSize = 3600;
        long[] ts = new long[totalRows];
        double[] close = new double[totalRows];
        long baseTs = 1711152000000L; // millis
        Random rng = new Random(42);
        for (int i = 0; i < totalRows; i++) {
            ts[i] = baseTs + i * 1000L;
            close[i] = Math.round((65000.0 + Math.sin(i * 0.001) * 500 + rng.nextDouble() * 10) * 100.0) / 100.0;
        }

        try (LastraWriter w = new LastraWriter(new FileOutputStream(dir.resolve("row-groups.lastra").toFile()))) {
            w.setRowGroupSize(rgSize);
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(totalRows, ts, close);
        }
    }

    // Streaming-append: K calls of writeSeries(rgSize, ...). Dual of the single-call,
    // auto-partitioned row-groups.lastra fixture. Validates that the footer's
    // seriesRowCount equals the SUM across calls (regression for 0.8.2 writer fix).
    private static void generateMultipleWriteSeriesCalls(Path dir) throws Exception {
        int rgSize = 100;
        int rgCount = 4;
        long t0 = 1_700_000_000_000L;
        long hourMs = 3_600_000L;

        try (LastraWriter w = new LastraWriter(new FileOutputStream(dir.resolve("multi-write.lastra").toFile()))) {
            w.setRowGroupSize(rgSize);
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("v", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            for (int g = 0; g < rgCount; g++) {
                long[] ts = new long[rgSize];
                double[] v = new double[rgSize];
                for (int i = 0; i < rgSize; i++) {
                    ts[i] = t0 + g * hourMs + i;
                    v[i] = 100.0 + g + i * 0.01;
                }
                w.writeSeries(rgSize, ts, v);
            }
        }
    }
}

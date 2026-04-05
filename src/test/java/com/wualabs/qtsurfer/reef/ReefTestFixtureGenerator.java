package com.wualabs.qtsurfer.reef;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;

/**
 * Generates .reef fixture files for cross-language testing (Java writer → TS reader).
 * Run with: mvn exec:java -Dexec.mainClass=...ReefTestFixtureGenerator -Dexec.args="/output/dir"
 */
public class ReefTestFixtureGenerator {

    public static void main(String[] args) throws Exception {
        Path outDir = args.length > 0 ? Path.of(args[0]) : Path.of("target/fixtures");
        Files.createDirectories(outDir);

        generateSeriesOnly(outDir);
        generateWithMetadata(outDir);
        generateWithEvents(outDir);

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

        try (ReefWriter w = new ReefWriter(new FileOutputStream(dir.resolve("series-only.reef").toFile()))) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Reef.DataType.DOUBLE, Reef.Codec.ALP);
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

        try (ReefWriter w = new ReefWriter(new FileOutputStream(dir.resolve("with-metadata.reef").toFile()))) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("ema1", Reef.DataType.DOUBLE, Reef.Codec.ALP,
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

        try (ReefWriter w = new ReefWriter(new FileOutputStream(dir.resolve("with-events.reef").toFile()))) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.addEventColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addEventColumn("type", Reef.DataType.BINARY, Reef.Codec.VARLEN);
            w.addEventColumn("data", Reef.DataType.BINARY, Reef.Codec.VARLEN_ZSTD);
            w.writeSeries(rows, ts, close);
            w.writeEvents(eventCount, eventTs, types, data);
        }
    }
}

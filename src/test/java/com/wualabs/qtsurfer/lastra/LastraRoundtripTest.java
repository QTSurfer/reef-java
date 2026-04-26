package com.wualabs.qtsurfer.lastra;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class LastraRoundtripTest {

    @Test
    void testSeriesOnlyTickerData() throws Exception {
        int rows = 3600;
        long[] ts = new long[rows];
        double[] close = new double[rows];
        Random rng = new Random(42);
        long baseTs = 1711152000000000000L; // ns
        for (int i = 0; i < rows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            close[i] = Math.round((65000.0 + Math.sin(i * 0.001) * 500 + rng.nextDouble() * 10) * 100.0) / 100.0;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(rows, ts, close);
        }

        byte[] lastraBytes = baos.toByteArray();
        double ratio = (double) (rows * 16) / lastraBytes.length;
        System.out.printf("Series only: %d rows, %d bytes, ratio=%.2fx%n", rows, lastraBytes.length, ratio);

        LastraReader r = LastraReader.from(lastraBytes);
        assertThat(r.seriesRowCount()).isEqualTo(rows);
        assertThat(r.seriesColumns()).hasSize(2);

        long[] gotTs = r.readSeriesLong("ts");
        double[] gotClose = r.readSeriesDouble("close");
        assertThat(gotTs).containsExactly(ts);
        for (int i = 0; i < rows; i++) {
            assertThat(Double.doubleToRawLongBits(gotClose[i]))
                    .as("close[%d]", i)
                    .isEqualTo(Double.doubleToRawLongBits(close[i]));
        }
    }

    @Test
    void testOhlcvData() throws Exception {
        int rows = 1000;
        long[] ts = new long[rows];
        double[] open = new double[rows];
        double[] high = new double[rows];
        double[] low = new double[rows];
        double[] close = new double[rows];
        double[] volume = new double[rows];
        Random rng = new Random(42);
        long baseTs = 1711152000000000000L;
        for (int i = 0; i < rows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            double base = 65000.0 + Math.sin(i * 0.001) * 500 + rng.nextDouble() * 10;
            close[i] = Math.round(base * 100.0) / 100.0;
            open[i] = Math.round((base - rng.nextDouble() * 5) * 100.0) / 100.0;
            high[i] = Math.round((base + rng.nextDouble() * 10) * 100.0) / 100.0;
            low[i] = Math.round((base - rng.nextDouble() * 10) * 100.0) / 100.0;
            volume[i] = Math.round(rng.nextDouble() * 100000 * 100.0) / 100.0;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("open", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("high", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("low", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addSeriesColumn("volume", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(rows, ts, open, high, low, close, volume);
        }

        byte[] lastraBytes = baos.toByteArray();
        double ratio = (double) (rows * 48) / lastraBytes.length;
        System.out.printf("OHLCV: %d rows, %d bytes, ratio=%.2fx%n", rows, lastraBytes.length, ratio);

        LastraReader r = LastraReader.from(lastraBytes);
        assertThat(r.readSeriesLong("ts")).containsExactly(ts);
        assertBitExact(r.readSeriesDouble("close"), close);
        assertBitExact(r.readSeriesDouble("volume"), volume);
    }

    @Test
    void testSeriesWithEvents() throws Exception {
        int rows = 500;
        long[] ts = new long[rows];
        double[] close = new double[rows];
        Random rng = new Random(42);
        long baseTs = 1711152000000000000L;
        for (int i = 0; i < rows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            close[i] = Math.round((65000.0 + rng.nextDouble() * 100) * 100.0) / 100.0;
        }

        // Events: 5 signals at specific timestamps
        int eventCount = 5;
        long[] eventTs = {
                baseTs + 50_000_000_000L,
                baseTs + 120_000_000_000L,
                baseTs + 200_000_000_000L,
                baseTs + 350_000_000_000L,
                baseTs + 480_000_000_000L
        };
        byte[][] eventTypes = {
                "BUY".getBytes(StandardCharsets.UTF_8),
                "SELL".getBytes(StandardCharsets.UTF_8),
                "BUY".getBytes(StandardCharsets.UTF_8),
                "STOP_LOSS".getBytes(StandardCharsets.UTF_8),
                "SELL".getBytes(StandardCharsets.UTF_8)
        };
        byte[][] eventData = {
                "{\"price\":65042.17,\"qty\":0.5}".getBytes(StandardCharsets.UTF_8),
                "{\"price\":65100.33,\"qty\":0.5}".getBytes(StandardCharsets.UTF_8),
                null,
                "{\"price\":64900.00,\"reason\":\"stop_hit\"}".getBytes(StandardCharsets.UTF_8),
                null
        };

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addEventColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addEventColumn("type", Lastra.DataType.BINARY, Lastra.Codec.VARLEN);
            w.addEventColumn("data", Lastra.DataType.BINARY, Lastra.Codec.VARLEN_ZSTD);
            w.writeSeries(rows, ts, close);
            w.writeEvents(eventCount, eventTs, eventTypes, eventData);
        }

        byte[] lastraBytes = baos.toByteArray();
        System.out.printf("Series+Events: %d series rows + %d events, %d bytes%n",
                rows, eventCount, lastraBytes.length);

        LastraReader r = LastraReader.from(lastraBytes);
        assertThat(r.seriesRowCount()).isEqualTo(rows);
        assertThat(r.eventsRowCount()).isEqualTo(eventCount);
        assertThat(r.readSeriesLong("ts")).containsExactly(ts);
        assertBitExact(r.readSeriesDouble("close"), close);

        assertThat(r.readEventLong("ts")).containsExactly(eventTs);
        byte[][] gotTypes = r.readEventBinary("type");
        assertThat(new String(gotTypes[0], StandardCharsets.UTF_8)).isEqualTo("BUY");
        assertThat(new String(gotTypes[3], StandardCharsets.UTF_8)).isEqualTo("STOP_LOSS");
        byte[][] gotData = r.readEventBinary("data");
        assertThat(gotData[2]).isNull();
        assertThat(gotData[4]).isNull();
        assertThat(new String(gotData[0], StandardCharsets.UTF_8)).contains("65042.17");
    }

    @Test
    void testColumnMetadata() throws Exception {
        int rows = 100;
        long[] ts = new long[rows];
        double[] ema = new double[rows];
        for (int i = 0; i < rows; i++) {
            ts[i] = 1711152000000000000L + i * 1_000_000_000L;
            ema[i] = Math.round((65000.0 + i * 0.1) * 100.0) / 100.0;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("ema1", Lastra.DataType.DOUBLE, Lastra.Codec.ALP,
                    Map.of("indicator", "ema", "periods", "10"));
            w.writeSeries(rows, ts, ema);
        }

        LastraReader r = LastraReader.from(baos.toByteArray());
        ColumnDescriptor emaCol = r.getSeriesColumn("ema1");
        assertThat(emaCol.metadata()).containsEntry("indicator", "ema");
        assertThat(emaCol.metadata()).containsEntry("periods", "10");
        assertBitExact(r.readSeriesDouble("ema1"), ema);
    }

    @Test
    void testReadFromInputStream() throws Exception {
        int rows = 50;
        long[] ts = new long[rows];
        double[] values = new double[rows];
        for (int i = 0; i < rows; i++) {
            ts[i] = i * 1000L;
            values[i] = i * 1.5;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("v", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(rows, ts, values);
        }

        LastraReader r = LastraReader.from(new ByteArrayInputStream(baos.toByteArray()));
        assertThat(r.readSeriesLong("ts")).containsExactly(ts);
        assertBitExact(r.readSeriesDouble("v"), values);
    }

    /**
     * Events with heterogeneous column lengths.
     *
     * <p>Simulates a strategy that emits buy signals (3), sell signals (2), and stop-loss (1).
     * All event columns share eventsRowCount = max(3, 2, 1) = 3. Shorter columns are padded
     * with zero/empty values. A "type" column identifies the event category.
     *
     * <p>Layout in memory:
     * <pre>
     *   row | ts                  | type      | price    | reason
     *   0   | 1711152050000000000 | BUY       | 65042.17 | (empty)
     *   1   | 1711152120000000000 | BUY       | 65100.33 | (empty)
     *   2   | 1711152200000000000 | BUY       | 64980.50 | (empty)
     *   --- padded from here for SELL (2 events) and STOP_LOSS (1 event) ---
     * </pre>
     *
     * <p>In practice, heterogeneous events are written as separate .lastra files
     * or merged into a single events section with the union of all columns,
     * using the highest count and padding shorter ones with defaults.
     */
    @Test
    void testHeterogeneousEventsPaddedToMaxCount() throws Exception {
        // Series: minimal
        int seriesRows = 100;
        long[] ts = new long[seriesRows];
        double[] close = new double[seriesRows];
        long baseTs = 1711152000000000000L;
        for (int i = 0; i < seriesRows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            close[i] = 65000.0 + i;
        }

        // 3 buy events, 2 sell events, 1 stop-loss = 6 total events
        // All share the same eventsRowCount = 6
        int eventCount = 6;
        long[] eventTs = {
                baseTs + 10_000_000_000L,   // BUY
                baseTs + 50_000_000_000L,   // BUY
                baseTs + 70_000_000_000L,   // BUY
                baseTs + 30_000_000_000L,   // SELL
                baseTs + 80_000_000_000L,   // SELL
                baseTs + 60_000_000_000L,   // STOP_LOSS
        };
        byte[][] eventTypes = {
                "BUY".getBytes(StandardCharsets.UTF_8),
                "BUY".getBytes(StandardCharsets.UTF_8),
                "BUY".getBytes(StandardCharsets.UTF_8),
                "SELL".getBytes(StandardCharsets.UTF_8),
                "SELL".getBytes(StandardCharsets.UTF_8),
                "STOP_LOSS".getBytes(StandardCharsets.UTF_8),
        };
        // price: all events have a price
        double[] eventPrices = {65042.17, 65100.33, 64980.50, 65200.00, 65150.75, 64900.00};
        // reason: only STOP_LOSS has a reason, others are empty
        byte[][] eventReasons = {
                new byte[0],  // BUY - no reason
                new byte[0],  // BUY
                new byte[0],  // BUY
                new byte[0],  // SELL
                new byte[0],  // SELL
                "{\"trigger\":\"trailing_stop\",\"pct\":2.5}".getBytes(StandardCharsets.UTF_8),
        };

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addEventColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addEventColumn("type", Lastra.DataType.BINARY, Lastra.Codec.VARLEN);
            w.addEventColumn("price", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.addEventColumn("reason", Lastra.DataType.BINARY, Lastra.Codec.VARLEN_ZSTD);
            w.writeSeries(seriesRows, ts, close);
            w.writeEvents(eventCount, eventTs, eventTypes, eventPrices, eventReasons);
        }

        LastraReader r = LastraReader.from(baos.toByteArray());
        assertThat(r.seriesRowCount()).isEqualTo(seriesRows);
        assertThat(r.eventsRowCount()).isEqualTo(eventCount);
        assertThat(r.eventColumns()).hasSize(4);

        // All 6 events readable
        long[] gotTs = r.readEventLong("ts");
        assertThat(gotTs).hasSize(eventCount);
        assertThat(gotTs).containsExactly(eventTs);

        // Types: all 6 present
        byte[][] gotTypes = r.readEventBinary("type");
        assertThat(new String(gotTypes[0], StandardCharsets.UTF_8)).isEqualTo("BUY");
        assertThat(new String(gotTypes[3], StandardCharsets.UTF_8)).isEqualTo("SELL");
        assertThat(new String(gotTypes[5], StandardCharsets.UTF_8)).isEqualTo("STOP_LOSS");

        // Prices: all 6 present
        double[] gotPrices = r.readEventDouble("price");
        assertBitExact(gotPrices, eventPrices);

        // Reason: only index 5 has content, rest are empty
        byte[][] gotReasons = r.readEventBinary("reason");
        assertThat(gotReasons[0]).isEmpty();
        assertThat(gotReasons[4]).isEmpty();
        assertThat(new String(gotReasons[5], StandardCharsets.UTF_8)).contains("trailing_stop");

        System.out.printf("Heterogeneous events: %d series + %d events (3 BUY + 2 SELL + 1 STOP_LOSS), %d bytes%n",
                seriesRows, eventCount, baos.size());
    }

    /**
     * Row groups: 10,000 rows auto-partitioned into RGs of 3600 (simulating hourly chunks).
     * Verifies: RG count, per-RG stats (tsMin/tsMax), selective RG read, full read via
     * readSeriesDouble (concatenates all RGs).
     */
    @Test
    void testRowGroupsAutoPartition() throws Exception {
        int totalRows = 10_000;
        int rgSize = 3600; // ~1 hour of 1s ticks
        long[] ts = new long[totalRows];
        double[] close = new double[totalRows];
        long baseTs = 1711152000000L; // millis
        Random rng = new Random(42);
        for (int i = 0; i < totalRows; i++) {
            ts[i] = baseTs + i * 1000L;
            close[i] = Math.round((65000.0 + Math.sin(i * 0.001) * 500 + rng.nextDouble() * 10) * 100.0) / 100.0;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.setRowGroupSize(rgSize);
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(totalRows, ts, close);
        }

        byte[] bytes = baos.toByteArray();
        LastraReader r = LastraReader.from(bytes);

        // 10000 / 3600 = 2 full + 1 partial = 3 row groups
        assertThat(r.rowGroupCount()).isEqualTo(3);
        assertThat(r.seriesRowCount()).isEqualTo(totalRows);

        // RG stats
        RowGroupStats rg0 = r.rowGroupStats(0);
        assertThat(rg0.rowCount()).isEqualTo(3600);
        assertThat(rg0.tsMin()).isEqualTo(ts[0]);
        assertThat(rg0.tsMax()).isEqualTo(ts[3599]);

        RowGroupStats rg1 = r.rowGroupStats(1);
        assertThat(rg1.rowCount()).isEqualTo(3600);
        assertThat(rg1.tsMin()).isEqualTo(ts[3600]);
        assertThat(rg1.tsMax()).isEqualTo(ts[7199]);

        RowGroupStats rg2 = r.rowGroupStats(2);
        assertThat(rg2.rowCount()).isEqualTo(2800); // remainder
        assertThat(rg2.tsMin()).isEqualTo(ts[7200]);
        assertThat(rg2.tsMax()).isEqualTo(ts[9999]);

        // Selective RG read: only RG 1 (hour 2)
        double[] rg1Close = r.readRowGroupDouble(1, "close");
        assertThat(rg1Close).hasSize(3600);
        for (int i = 0; i < 3600; i++) {
            assertThat(Double.doubleToRawLongBits(rg1Close[i]))
                    .isEqualTo(Double.doubleToRawLongBits(close[3600 + i]));
        }

        // Full read (concatenates all RGs)
        long[] allTs = r.readSeriesLong("ts");
        assertThat(allTs).containsExactly(ts);
        double[] allClose = r.readSeriesDouble("close");
        assertBitExact(allClose, close);

        // Temporal filter: find RGs overlapping [ts[4000], ts[5000]]
        long queryFrom = ts[4000];
        long queryTo = ts[5000];
        int matchedRgs = 0;
        for (int i = 0; i < r.rowGroupCount(); i++) {
            RowGroupStats s = r.rowGroupStats(i);
            if (s.tsMax() >= queryFrom && s.tsMin() <= queryTo) {
                matchedRgs++;
            }
        }
        assertThat(matchedRgs).isEqualTo(1); // only RG 1 (3600-7199) overlaps

        System.out.printf("Row groups: %d rows → %d RGs (size %d), %d bytes, query matched %d RG%n",
                totalRows, r.rowGroupCount(), rgSize, bytes.length, matchedRgs);
    }

    /**
     * Files written by lastra-java ≤ 0.8.0 (and the lastra-convert ≤ 0.12.0
     * runs that pulled that build through JitPack) ended with just the 4-byte
     * FOOTER_MAGIC, no size hint. The HEAD reader's row-groups branch used to
     * unconditionally read {@code data.length - 4} as a footerSize, which on
     * legacy files was actually the magic — yielding a wildly negative
     * {@code footerPos} and an ArrayIndexOutOfBoundsException at construction
     * time. Production frate publisher hit this on 599/2368 instruments per
     * hourly run before the fix.
     */
    @Test
    void testRowGroupsWithLegacyFourByteTrailer() throws Exception {
        int totalRows = 5000;
        int rgSize = 2000;
        long[] ts = new long[totalRows];
        double[] close = new double[totalRows];
        long baseTs = 1711152000000L;
        for (int i = 0; i < totalRows; i++) {
            ts[i] = baseTs + i * 1000L;
            close[i] = 100.0 + i * 0.01;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.setRowGroupSize(rgSize);
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(totalRows, ts, close);
        }
        byte[] modern = baos.toByteArray();
        // Verify the modern writer emits the 8-byte trailer.
        assertThat(modern.length).isGreaterThan(8);

        // Strip the 4-byte size hint to simulate a legacy file. The trailer
        // becomes just the FOOTER_MAGIC.
        byte[] legacy = new byte[modern.length - 4];
        System.arraycopy(modern, 0, legacy, 0, modern.length - 4);
        // Sanity: last 4 bytes are the magic.
        assertThat((legacy[legacy.length - 4] & 0xFF)
                | ((legacy[legacy.length - 3] & 0xFF) << 8)
                | ((legacy[legacy.length - 2] & 0xFF) << 16)
                | ((legacy[legacy.length - 1] & 0xFF) << 24))
                .isEqualTo(Lastra.FOOTER_MAGIC);

        LastraReader r = LastraReader.from(legacy);
        assertThat(r.seriesRowCount()).isEqualTo(totalRows);
        assertThat(r.rowGroupCount()).isEqualTo(3); // 5000 / 2000 = 2 + 1
        assertThat(r.readSeriesLong("ts")).containsExactly(ts);
        assertBitExact(r.readSeriesDouble("close"), close);
    }

    /**
     * Regression for the bug that produced doubled lastra files in production:
     * lastra-convert ≤ 0.12.0 called {@code w.close()} explicitly inside a
     * try-with-resources block, so close() ran twice and emitted the body
     * twice. Files written this way grew to 2× their expected size and broke
     * any reader that walked them as a single Lastra blob.
     */
    @Test
    void testCloseIsIdempotent() throws Exception {
        long[] ts = {1, 2, 3};
        double[] v = {1.0, 2.0, 3.0};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("v", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(3, ts, v);
            w.close();
            // Try-with-resources will call close() again on exit. Both calls
            // together must produce a single, well-formed file.
        }
        byte[] singleClose = baos.toByteArray();

        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(baos2)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("v", Lastra.DataType.DOUBLE, Lastra.Codec.ALP);
            w.writeSeries(3, ts, v);
            // No explicit close — let try-with-resources do it.
        }
        byte[] autoClose = baos2.toByteArray();

        assertThat(singleClose).isEqualTo(autoClose);
        LastraReader r = LastraReader.from(singleClose);
        assertThat(r.seriesRowCount()).isEqualTo(3);
        assertThat(r.readSeriesLong("ts")).containsExactly(ts);
    }

    private static void assertBitExact(double[] actual, double[] expected) {
        assertThat(actual).hasSize(expected.length);
        for (int i = 0; i < expected.length; i++) {
            assertThat(Double.doubleToRawLongBits(actual[i]))
                    .as("value[%d] expected=%s got=%s", i, expected[i], actual[i])
                    .isEqualTo(Double.doubleToRawLongBits(expected[i]));
        }
    }
}

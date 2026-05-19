package com.wualabs.qtsurfer.lastra;

import com.wualabs.qtsurfer.alp.AlpCompressor;
import com.wualabs.qtsurfer.lastra.codec.DeltaVarintCodec;
import com.wualabs.qtsurfer.lastra.codec.GorillaCodec;
import com.wualabs.qtsurfer.lastra.codec.PongoCodec;
import com.wualabs.qtsurfer.lastra.codec.RawCodec;
import com.wualabs.qtsurfer.lastra.codec.VarlenCodec;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Writes Lastra files: header + series columns + optional events columns + footer.
 *
 * <p>Usage:
 * <pre>
 * try (LastraWriter w = new LastraWriter(outputStream)) {
 *     w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
 *     w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
 *     w.addEventColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
 *     w.addEventColumn("type", DataType.BINARY, Codec.VARLEN);
 *
 *     w.beginSeries(rowCount);
 *     // write series data...
 *
 *     w.beginEvents(eventCount);
 *     // write event data...
 * }
 * </pre>
 */
public class LastraWriter implements Closeable {

    private final OutputStream out;
    private final List<ColumnDescriptor> seriesColumns = new ArrayList<>();
    private final List<ColumnDescriptor> eventColumns = new ArrayList<>();

    // Events still buffer their raw column data (close() reads from these for events output).
    // Series went to row-group-only retention in 0.8.3 — see writeSeries() for the removed
    // dead block.
    private final List<long[]> eventLongBuffers = new ArrayList<>();
    private final List<double[]> eventDoubleBuffers = new ArrayList<>();
    private final List<byte[][]> eventBinaryBuffers = new ArrayList<>();

    // Row groups: each entry is a pre-compressed row group with its stats
    private final List<RowGroupData> rowGroups = new ArrayList<>();

    private int seriesRowCount;
    private int eventsRowCount;
    private int rowGroupSize = DEFAULT_ROW_GROUP_SIZE;
    private boolean closed;

    /** Default row group size in rows. */
    public static final int DEFAULT_ROW_GROUP_SIZE = 4096;

    private static final class RowGroupData {
        final int rowCount;
        final long tsMin, tsMax;
        final List<byte[]> compressedColumns;
        final List<Integer> crcs;
        RowGroupData(int rowCount, long tsMin, long tsMax,
                     List<byte[]> compressedColumns, List<Integer> crcs) {
            this.rowCount = rowCount; this.tsMin = tsMin; this.tsMax = tsMax;
            this.compressedColumns = compressedColumns; this.crcs = crcs;
        }
    }

    public LastraWriter(OutputStream out) {
        this.out = out;
    }

    public LastraWriter addSeriesColumn(String name, Lastra.DataType dataType, Lastra.Codec codec) {
        return addSeriesColumn(name, dataType, codec, null);
    }

    public LastraWriter addSeriesColumn(String name, Lastra.DataType dataType, Lastra.Codec codec,
                                      Map<String, String> metadata) {
        seriesColumns.add(new ColumnDescriptor(name, dataType, codec, metadata));
        return this;
    }

    public LastraWriter addEventColumn(String name, Lastra.DataType dataType, Lastra.Codec codec) {
        return addEventColumn(name, dataType, codec, null);
    }

    public LastraWriter addEventColumn(String name, Lastra.DataType dataType, Lastra.Codec codec,
                                     Map<String, String> metadata) {
        eventColumns.add(new ColumnDescriptor(name, dataType, codec, metadata));
        return this;
    }

    /**
     * Sets the row group size. Data passed to {@link #writeSeries} is automatically partitioned
     * into row groups of this size. Default: {@value #DEFAULT_ROW_GROUP_SIZE}.
     */
    public LastraWriter setRowGroupSize(int size) {
        this.rowGroupSize = size;
        return this;
    }

    /**
     * Set series data from arrays, auto-partitioned into row groups.
     *
     * <p>The first LONG column is assumed to be the timestamp for row group statistics.
     * Each array corresponds to a series column in order. Use {@code long[]} for LONG,
     * {@code double[]} for DOUBLE, {@code byte[][]} for BINARY.
     */
    public LastraWriter writeSeries(int rowCount, Object... columnData) {
        // Accumulate across calls — each invocation appends rows to the same series rather
        // than replacing it. The footer's series row count must reflect the total written.
        this.seriesRowCount += rowCount;

        // Partition into row groups
        for (int start = 0; start < rowCount; start += rowGroupSize) {
            int end = Math.min(start + rowGroupSize, rowCount);
            int rgRows = end - start;

            // Slice arrays for this row group
            Object[] slice = new Object[columnData.length];
            long tsMin = Long.MAX_VALUE, tsMax = Long.MIN_VALUE;

            for (int i = 0; i < seriesColumns.size(); i++) {
                ColumnDescriptor col = seriesColumns.get(i);
                switch (col.dataType()) {
                    case LONG: {
                        long[] src = (long[]) columnData[i];
                        long[] dst = new long[rgRows];
                        System.arraycopy(src, start, dst, 0, rgRows);
                        slice[i] = dst;
                        // First LONG column = timestamp for stats
                        if (tsMin == Long.MAX_VALUE) {
                            tsMin = dst[0];
                            tsMax = dst[rgRows - 1];
                        }
                        break;
                    }
                    case DOUBLE: {
                        double[] src = (double[]) columnData[i];
                        double[] dst = new double[rgRows];
                        System.arraycopy(src, start, dst, 0, rgRows);
                        slice[i] = dst;
                        break;
                    }
                    case BINARY: {
                        byte[][] src = (byte[][]) columnData[i];
                        byte[][] dst = new byte[rgRows][];
                        System.arraycopy(src, start, dst, 0, rgRows);
                        slice[i] = dst;
                        break;
                    }
                }
            }

            // Compress and buffer this row group
            List<byte[]> compressed = compressRowGroup(slice, rgRows);
            List<Integer> crcs = new ArrayList<>();
            for (byte[] col : compressed) crcs.add(crc32(col));
            rowGroups.add(new RowGroupData(rgRows, tsMin, tsMax, compressed, crcs));
        }

        // 0.8.3 (#93b streaming): the series{Long,Double,Binary}Buffers retention block
        // that lived here was dead — close() never reads from them for series output (it
        // serialises from `rowGroups`). The block held one Object reference per writeSeries()
        // call × N columns, which for streaming callers (lastra-convert >= 0.14 with one
        // writeSeries per parquet row-group) doubled heap residency for nothing. Removed.
        // Events path still uses event*Buffers and is untouched.
        return this;
    }

    private List<byte[]> compressRowGroup(Object[] columnData, int rowCount) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < seriesColumns.size(); i++) {
            ColumnDescriptor col = seriesColumns.get(i);
            switch (col.dataType()) {
                case LONG:
                    result.add(compressLongColumn((long[]) columnData[i], rowCount, col.codec()));
                    break;
                case DOUBLE:
                    result.add(compressDoubleColumn((double[]) columnData[i], rowCount, col.codec()));
                    break;
                case BINARY:
                    result.add(compressBinaryColumn((byte[][]) columnData[i], rowCount, col.codec()));
                    break;
            }
        }
        return result;
    }

    /**
     * Set events data from arrays. Same convention as {@link #writeSeries}.
     */
    public LastraWriter writeEvents(int rowCount, Object... columnData) {
        this.eventsRowCount = rowCount;
        for (int i = 0; i < eventColumns.size(); i++) {
            ColumnDescriptor col = eventColumns.get(i);
            switch (col.dataType()) {
                case LONG:
                    eventLongBuffers.add((long[]) columnData[i]);
                    eventDoubleBuffers.add(null);
                    eventBinaryBuffers.add(null);
                    break;
                case DOUBLE:
                    eventLongBuffers.add(null);
                    eventDoubleBuffers.add((double[]) columnData[i]);
                    eventBinaryBuffers.add(null);
                    break;
                case BINARY:
                    eventLongBuffers.add(null);
                    eventDoubleBuffers.add(null);
                    eventBinaryBuffers.add((byte[][]) columnData[i]);
                    break;
            }
        }
        return this;
    }

    @Override
    public void close() throws IOException {
        // Idempotent. Production caller pattern (lastra-convert ≤ 0.12.0)
        // accidentally invoked close() explicitly inside a try-with-resources,
        // which silently doubled the file contents for every backfill output.
        // Multiple invocations now no-op after the first.
        if (closed) return;
        closed = true;

        boolean hasEvents = !eventColumns.isEmpty() && eventsRowCount > 0;
        boolean hasRowGroups = rowGroups.size() > 1;
        int flags = Lastra.FLAG_HAS_FOOTER | Lastra.FLAG_HAS_CHECKSUMS;
        if (hasEvents) flags |= Lastra.FLAG_HAS_EVENTS;
        if (hasRowGroups) flags |= Lastra.FLAG_HAS_ROW_GROUPS;

        // 0.8.3 (#93b streaming): write directly to `out` (wrapped) instead of accumulating
        // the whole body in a ByteArrayOutputStream first. The old path OOMed on lastra-convert
        // 0.14 streaming inputs because BAOS would buffer 2832+ row-groups worth of compressed
        // column bytes + grow-doubling amplification before the single flush at the end.
        // CountingOutputStream gives us absolute byte offsets without needing to seek back.
        BufferedOutputStream buffered = new BufferedOutputStream(out, 64 * 1024);
        CountingOutputStream body = new CountingOutputStream(buffered);

        // === HEADER ===
        ByteBuffer header = ByteBuffer.allocate(22).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(Lastra.MAGIC);
        header.putShort((short) Lastra.VERSION);
        header.putShort((short) flags);
        header.putInt(seriesRowCount);
        header.putInt(seriesColumns.size());
        header.putInt(eventsRowCount);
        header.putShort((short) eventColumns.size());
        body.write(header.array());

        // === COLUMN DESCRIPTORS ===
        writeColumnDescriptors(body, seriesColumns);
        if (hasEvents) {
            writeColumnDescriptors(body, eventColumns);
        }

        long dataStart = body.position;

        // === SERIES DATA (row groups) ===
        // Gather offsets as we write — keep them in memory until footer time (1 int per RG).
        int[] rgOffsets = new int[rowGroups.size()];
        if (hasRowGroups) {
            // Multiple row groups: write each RG's columns sequentially
            for (int i = 0; i < rowGroups.size(); i++) {
                RowGroupData rg = rowGroups.get(i);
                rgOffsets[i] = (int) (body.position - dataStart);
                for (byte[] colData : rg.compressedColumns) {
                    writeIntLE(body, colData.length);
                    body.write(colData);
                }
            }
        } else if (!rowGroups.isEmpty()) {
            // Single row group: write as flat columns (backward compat)
            RowGroupData rg = rowGroups.get(0);
            for (byte[] colData : rg.compressedColumns) {
                writeIntLE(body, colData.length);
                body.write(colData);
            }
        }

        // === EVENTS DATA ===
        List<Integer> eventOffsets = new ArrayList<>();
        List<Integer> eventCrcs = new ArrayList<>();
        if (hasEvents) {
            List<byte[]> eventsCompressed = compressColumns(eventColumns,
                    eventLongBuffers, eventDoubleBuffers, eventBinaryBuffers, eventsRowCount);
            for (byte[] colData : eventsCompressed) {
                eventOffsets.add((int) (body.position - dataStart));
                writeIntLE(body, colData.length);
                body.write(colData);
                eventCrcs.add(crc32(colData));
            }
        }

        // === FOOTER ===
        long footerStart = body.position;
        if (hasRowGroups) {
            // Row group metadata
            writeIntLE(body, rowGroups.size());
            for (int i = 0; i < rowGroups.size(); i++) {
                RowGroupData rg = rowGroups.get(i);
                writeIntLE(body, rgOffsets[i]);      // byte offset
                writeIntLE(body, rg.rowCount);      // rows in this RG
                writeLongLE(body, rg.tsMin);         // min timestamp
                writeLongLE(body, rg.tsMax);         // max timestamp
            }
            // Per-RG per-column CRCs
            for (RowGroupData rg : rowGroups) {
                for (int crc : rg.crcs) writeIntLE(body, crc);
            }
        } else if (!rowGroups.isEmpty()) {
            // Single RG: write flat column offsets + CRCs (backward compat)
            // Format: [series offsets][event offsets][series CRCs][event CRCs]
            RowGroupData rg = rowGroups.get(0);
            int pos = 0;
            for (byte[] colData : rg.compressedColumns) {
                writeIntLE(body, pos);
                pos += 4 + colData.length;
            }
            for (int offset : eventOffsets) writeIntLE(body, offset);
            for (int crc : rg.crcs) writeIntLE(body, crc);
            for (int crc : eventCrcs) writeIntLE(body, crc);
        } else {
            // No data at all — just event offsets+CRCs if any
            for (int offset : eventOffsets) writeIntLE(body, offset);
            for (int crc : eventCrcs) writeIntLE(body, crc);
        }

        int footerSize = (int) (body.position - footerStart);
        writeIntLE(body, Lastra.FOOTER_MAGIC);
        // Footer size hint: allows HTTP Range clients to fetch footer in 2 requests
        // (read last 8 bytes → LAS! + footerSize → read footerSize bytes)
        writeIntLE(body, footerSize);

        buffered.flush();
        out.flush();
    }

    private void writeColumnDescriptors(OutputStream out, List<ColumnDescriptor> columns)
            throws IOException {
        for (ColumnDescriptor col : columns) {
            out.write(col.codec().id);
            out.write(col.dataType().id);
            int colFlags = 0;
            if (col.hasMetadata()) colFlags |= 0x02;
            out.write(colFlags);
            byte[] nameBytes = col.name().getBytes(StandardCharsets.UTF_8);
            out.write(nameBytes.length);
            out.write(nameBytes);
            if (col.hasMetadata()) {
                byte[] metaBytes = mapToJson(col.metadata()).getBytes(StandardCharsets.UTF_8);
                writeShortLE(out, metaBytes.length);
                out.write(metaBytes);
            }
        }
    }

    private List<byte[]> compressColumns(List<ColumnDescriptor> columns,
                                          List<long[]> longBufs, List<double[]> doubleBufs,
                                          List<byte[][]> binaryBufs, int rowCount) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor col = columns.get(i);
            switch (col.dataType()) {
                case LONG:
                    result.add(compressLongColumn(longBufs.get(i), rowCount, col.codec()));
                    break;
                case DOUBLE:
                    result.add(compressDoubleColumn(doubleBufs.get(i), rowCount, col.codec()));
                    break;
                case BINARY:
                    result.add(compressBinaryColumn(binaryBufs.get(i), rowCount, col.codec()));
                    break;
            }
        }
        return result;
    }

    private byte[] compressLongColumn(long[] data, int count, Lastra.Codec codec) {
        switch (codec) {
            case DELTA_VARINT: return DeltaVarintCodec.encode(data, count);
            case RAW: return RawCodec.encodeLongs(data, count);
            default: throw new IllegalArgumentException("Unsupported codec for LONG: " + codec);
        }
    }

    private byte[] compressDoubleColumn(double[] data, int count, Lastra.Codec codec) {
        switch (codec) {
            case ALP: return new AlpCompressor().compress(data, count);
            case GORILLA: return GorillaCodec.encode(data, count);
            case PONGO: return PongoCodec.encode(data, count);
            case RAW: return RawCodec.encodeDoubles(data, count);
            default: throw new IllegalArgumentException("Unsupported codec for DOUBLE: " + codec);
        }
    }

    private byte[] compressBinaryColumn(byte[][] data, int count, Lastra.Codec codec) {
        switch (codec) {
            case VARLEN: return VarlenCodec.encode(data, count, VarlenCodec.COMPRESSION_NONE);
            case VARLEN_ZSTD: return VarlenCodec.encode(data, count, VarlenCodec.COMPRESSION_ZSTD);
            case VARLEN_GZIP: return VarlenCodec.encode(data, count, VarlenCodec.COMPRESSION_GZIP);
            default: throw new IllegalArgumentException("Unsupported codec for BINARY: " + codec);
        }
    }

    private static int crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    private static void writeLongLE(OutputStream out, long value) throws IOException {
        writeIntLE(out, (int) value);
        writeIntLE(out, (int) (value >>> 32));
    }

    private static void writeIntLE(OutputStream out, int value) throws IOException {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    private static void writeShortLE(OutputStream out, int value) throws IOException {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
    }

    /** Counter wrapper used by streaming close() to track absolute byte offsets without
     * accumulating the body in heap (#93b: lastra-convert 0.14 streams 2832+ row-groups). */
    private static final class CountingOutputStream extends java.io.FilterOutputStream {
        long position;
        CountingOutputStream(OutputStream out) { super(out); }
        @Override public void write(int b) throws IOException { out.write(b); position++; }
        @Override public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len); position += len;
        }
    }

    private static String mapToJson(Map<String, String> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(e.getKey()).append("\":\"").append(e.getValue()).append("\"");
            first = false;
        }
        return sb.append("}").toString();
    }
}

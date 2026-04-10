package com.wualabs.qtsurfer.lastra;

import com.wualabs.qtsurfer.alp.AlpCompressor;
import com.wualabs.qtsurfer.lastra.codec.DeltaVarintCodec;
import com.wualabs.qtsurfer.lastra.codec.GorillaCodec;
import com.wualabs.qtsurfer.lastra.codec.PongoCodec;
import com.wualabs.qtsurfer.lastra.codec.RawCodec;
import com.wualabs.qtsurfer.lastra.codec.VarlenCodec;
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

    // Buffered column data (accumulated during write phase)
    private final List<long[]> seriesLongBuffers = new ArrayList<>();
    private final List<double[]> seriesDoubleBuffers = new ArrayList<>();
    private final List<byte[][]> seriesBinaryBuffers = new ArrayList<>();
    private final List<long[]> eventLongBuffers = new ArrayList<>();
    private final List<double[]> eventDoubleBuffers = new ArrayList<>();
    private final List<byte[][]> eventBinaryBuffers = new ArrayList<>();

    // Row groups: each entry is a pre-compressed row group with its stats
    private final List<RowGroupData> rowGroups = new ArrayList<>();

    private int seriesRowCount;
    private int eventsRowCount;
    private int rowGroupSize = DEFAULT_ROW_GROUP_SIZE;

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
        this.seriesRowCount = rowCount;

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

        // Also store raw buffers for single-RG backward compat path (not used when rowGroups > 0)
        for (int i = 0; i < seriesColumns.size(); i++) {
            ColumnDescriptor col = seriesColumns.get(i);
            switch (col.dataType()) {
                case LONG:
                    seriesLongBuffers.add((long[]) columnData[i]);
                    seriesDoubleBuffers.add(null);
                    seriesBinaryBuffers.add(null);
                    break;
                case DOUBLE:
                    seriesLongBuffers.add(null);
                    seriesDoubleBuffers.add((double[]) columnData[i]);
                    seriesBinaryBuffers.add(null);
                    break;
                case BINARY:
                    seriesLongBuffers.add(null);
                    seriesDoubleBuffers.add(null);
                    seriesBinaryBuffers.add((byte[][]) columnData[i]);
                    break;
            }
        }
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
        boolean hasEvents = !eventColumns.isEmpty() && eventsRowCount > 0;
        boolean hasRowGroups = rowGroups.size() > 1;
        int flags = Lastra.FLAG_HAS_FOOTER | Lastra.FLAG_HAS_CHECKSUMS;
        if (hasEvents) flags |= Lastra.FLAG_HAS_EVENTS;
        if (hasRowGroups) flags |= Lastra.FLAG_HAS_ROW_GROUPS;

        ByteArrayOutputStream body = new ByteArrayOutputStream();

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

        int dataStart = body.size();

        // === SERIES DATA (row groups) ===
        List<Integer> rgOffsets = new ArrayList<>();
        if (hasRowGroups) {
            // Multiple row groups: write each RG's columns sequentially
            for (RowGroupData rg : rowGroups) {
                rgOffsets.add(body.size() - dataStart);
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
                eventOffsets.add(body.size() - dataStart);
                writeIntLE(body, colData.length);
                body.write(colData);
                eventCrcs.add(crc32(colData));
            }
        }

        // === FOOTER ===
        if (hasRowGroups) {
            // Row group metadata
            writeIntLE(body, rowGroups.size());
            for (int i = 0; i < rowGroups.size(); i++) {
                RowGroupData rg = rowGroups.get(i);
                writeIntLE(body, rgOffsets.get(i));   // byte offset
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

        writeIntLE(body, Lastra.FOOTER_MAGIC);

        out.write(body.toByteArray());
        out.flush();
    }

    private void writeColumnDescriptors(ByteArrayOutputStream out, List<ColumnDescriptor> columns)
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

    private static void writeLongLE(ByteArrayOutputStream out, long value) {
        writeIntLE(out, (int) value);
        writeIntLE(out, (int) (value >>> 32));
    }

    private static void writeIntLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    private static void writeShortLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
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

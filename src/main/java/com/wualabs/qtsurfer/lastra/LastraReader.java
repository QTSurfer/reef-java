package com.wualabs.qtsurfer.lastra;

import com.wualabs.qtsurfer.alp.AlpDecompressor;
import com.wualabs.qtsurfer.lastra.codec.DeltaVarintCodec;
import com.wualabs.qtsurfer.lastra.codec.GorillaCodec;
import com.wualabs.qtsurfer.lastra.codec.PongoCodec;
import com.wualabs.qtsurfer.lastra.codec.RawCodec;
import com.wualabs.qtsurfer.lastra.codec.VarlenCodec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Reads Lastra files. Supports selective column reading via footer offsets.
 *
 * <p>Usage:
 * <pre>
 * LastraReader reader = LastraReader.from(inputStream);
 * long[] ts = reader.readSeriesLong("ts");
 * double[] close = reader.readSeriesDouble("close");
 * byte[][] signals = reader.readEventBinary("data");
 * </pre>
 */
public class LastraReader {

    private final ByteBuffer buf;
    private final int flags;
    private final int seriesRowCount;
    private final int eventsRowCount;
    private final List<ColumnDescriptor> seriesColumns;
    private final List<ColumnDescriptor> eventColumns;
    private final int dataOffset;
    private final int[] seriesOffsets;
    private final int[] eventOffsets;

    // Cached column data regions (offset + length in buf)
    private final int[] seriesDataPos;
    private final int[] seriesDataLen;
    private final int[] eventDataPos;
    private final int[] eventDataLen;

    // Per-column CRC32 checksums (empty if file has no checksums)
    private final int[] seriesCrcs;
    private final int[] eventCrcs;
    private final boolean hasChecksums;

    // Row groups (empty if single implicit RG)
    private final List<RowGroupStats> rowGroupStatsList;
    private final List<int[]> rgColPos;  // per-RG: position of each column's data
    private final List<int[]> rgColLen;  // per-RG: length of each column's compressed data
    private final List<int[]> rgColCrcs; // per-RG: CRC per column

    private LastraReader(byte[] data) {
        this.buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Header
        int magic = buf.getInt();
        if (magic != Lastra.MAGIC) {
            throw new IllegalArgumentException(
                    String.format("Not a Lastra file (magic: 0x%08X)", magic));
        }
        int version = buf.getShort() & 0xFFFF;
        if (version > Lastra.VERSION) {
            throw new IllegalArgumentException("Unsupported Lastra version: " + version);
        }
        this.flags = buf.getShort() & 0xFFFF;
        this.seriesRowCount = buf.getInt();
        int seriesColCount = buf.getInt();
        this.eventsRowCount = buf.getInt();
        int eventColCount = buf.getShort() & 0xFFFF;

        // Column descriptors
        this.seriesColumns = readColumnDescriptors(seriesColCount);
        boolean hasEvents = (flags & Lastra.FLAG_HAS_EVENTS) != 0;
        this.eventColumns = hasEvents ? readColumnDescriptors(eventColCount) : Collections.emptyList();

        // Data section starts here
        this.dataOffset = buf.position();

        // Read footer
        boolean hasFooter = (flags & Lastra.FLAG_HAS_FOOTER) != 0;
        this.hasChecksums = (flags & Lastra.FLAG_HAS_CHECKSUMS) != 0;
        boolean hasRowGroups = (flags & Lastra.FLAG_HAS_ROW_GROUPS) != 0;

        this.rowGroupStatsList = new ArrayList<>();
        this.rgColPos = new ArrayList<>();
        this.rgColLen = new ArrayList<>();
        this.rgColCrcs = new ArrayList<>();

        // Trailer detection. Two layouts coexist in the wild:
        //   * pre-size-hint (lastra-java tag 0.8.0 and lastra-convert ≤ 0.12.0):
        //     last 4 bytes are FOOTER_MAGIC and that's it
        //   * with size hint (current writer): last 8 bytes are
        //     [FOOTER_MAGIC][footerSize LE], so HTTP-Range clients can grab
        //     the footer in a second request.
        // We try the modern layout first, then fall back to the legacy one.
        int trailerSize;
        boolean hasFooterSizeHint = false;
        if (data.length >= 8 && getIntLE(data, data.length - 8) == Lastra.FOOTER_MAGIC) {
            hasFooterSizeHint = true;
            trailerSize = 8;
        } else if (data.length >= 4 && getIntLE(data, data.length - 4) == Lastra.FOOTER_MAGIC) {
            trailerSize = 4;
        } else {
            // No magic at either position. Default to 8 so existing offsets stay
            // consistent; column reads will fail with a clearer CRC error if the
            // file is genuinely corrupt.
            trailerSize = 8;
        }

        if (hasFooter && hasRowGroups) {
            // Two paths to locate the footer depending on whether the writer
            // emitted a size hint:
            //   * With hint:  footerPos = length - 8 - footerSize (cheap, exact)
            //   * Without:    forward-scan data section group-by-group; the
            //                 first byte that isn't a column-length prefix is
            //                 the start of the footer. Used by lastra-java
            //                 ≤ 0.8.0 / lastra-convert files in production.
            int eventCols = eventColumns.size();
            int footerPos;
            int rgCount;

            if (hasFooterSizeHint) {
                int footerSize = getIntLE(data, data.length - 4);
                footerPos = data.length - trailerSize - footerSize;
                rgCount = getIntLE(data, footerPos);
            } else {
                int[] scanResult = scanRowGroupsForFooter(data, dataOffset, seriesColCount,
                        eventCols, hasChecksums, trailerSize);
                footerPos = scanResult[0];
                rgCount = scanResult[1];
            }

            // Parse footer: [rgCount][rg stats...][rg CRCs...][event offsets][event CRCs]
            int fp = footerPos + 4; // skip rgCount we already read

            for (int i = 0; i < rgCount; i++) {
                int rgOffset = getIntLE(data, fp); fp += 4;
                int rgRows = getIntLE(data, fp); fp += 4;
                long rgTsMin = getLongLE(data, fp); fp += 8;
                long rgTsMax = getLongLE(data, fp); fp += 8;
                rowGroupStatsList.add(new RowGroupStats(rgRows, rgOffset, rgTsMin, rgTsMax));
            }

            if (hasChecksums) {
                for (int i = 0; i < rgCount; i++) {
                    int[] crcs = new int[seriesColCount];
                    for (int c = 0; c < seriesColCount; c++) {
                        crcs[c] = getIntLE(data, fp); fp += 4;
                    }
                    rgColCrcs.add(crcs);
                }
            }

            this.eventOffsets = new int[eventCols];
            this.eventCrcs = new int[eventCols];
            for (int i = 0; i < eventCols; i++) { eventOffsets[i] = getIntLE(data, fp); fp += 4; }
            if (hasChecksums) {
                for (int i = 0; i < eventCols; i++) { eventCrcs[i] = getIntLE(data, fp); fp += 4; }
            }

            // Scan RG data forward using rgCount from footer
            int scanPos = dataOffset;
            for (int rg = 0; rg < rgCount; rg++) {
                int[] colPos = new int[seriesColCount];
                int[] colLen = new int[seriesColCount];
                for (int c = 0; c < seriesColCount; c++) {
                    int len = getIntLE(data, scanPos);
                    colPos[c] = scanPos + 4;
                    colLen[c] = len;
                    scanPos += 4 + len;
                }
                rgColPos.add(colPos);
                rgColLen.add(colLen);
            }

            // Events data
            this.eventDataPos = new int[eventCols];
            this.eventDataLen = new int[eventCols];
            for (int i = 0; i < eventCols; i++) {
                int len = getIntLE(data, scanPos);
                eventDataPos[i] = scanPos + 4;
                eventDataLen[i] = len;
                scanPos += 4 + len;
            }

            this.seriesDataPos = new int[0];
            this.seriesDataLen = new int[0];
            this.seriesOffsets = new int[0];
            this.seriesCrcs = new int[0];

        } else if (hasFooter) {
            // No row groups: original flat format
            int totalCols = seriesColCount + eventColumns.size();
            int footerInts = totalCols; // offsets
            if (hasChecksums) footerInts += totalCols; // + CRCs

            // Footer ends before the trailer (LAS! + footerSize = 8 bytes, or just LAS! = 4 bytes)
            int footerStart = data.length - trailerSize - footerInts * 4;
            ByteBuffer footer = ByteBuffer.wrap(data, footerStart, footerInts * 4)
                    .order(ByteOrder.LITTLE_ENDIAN);

            this.seriesOffsets = new int[seriesColCount];
            for (int i = 0; i < seriesColCount; i++) {
                seriesOffsets[i] = footer.getInt();
            }
            this.eventOffsets = new int[eventColumns.size()];
            for (int i = 0; i < eventColumns.size(); i++) {
                eventOffsets[i] = footer.getInt();
            }

            if (hasChecksums) {
                this.seriesCrcs = new int[seriesColCount];
                for (int i = 0; i < seriesColCount; i++) {
                    seriesCrcs[i] = footer.getInt();
                }
                this.eventCrcs = new int[eventColumns.size()];
                for (int i = 0; i < eventColumns.size(); i++) {
                    eventCrcs[i] = footer.getInt();
                }
            } else {
                this.seriesCrcs = new int[0];
                this.eventCrcs = new int[0];
            }

            // LAS! magic already verified via trailer detection above

            // Precompute column data positions by scanning length prefixes
            this.seriesDataPos = new int[seriesColCount];
            this.seriesDataLen = new int[seriesColCount];
            int pos = dataOffset;
            for (int i = 0; i < seriesColCount; i++) {
                int len = getIntLE(data, pos);
                seriesDataPos[i] = pos + 4;
                seriesDataLen[i] = len;
                pos += 4 + len;
            }
            this.eventDataPos = new int[eventColumns.size()];
            this.eventDataLen = new int[eventColumns.size()];
            for (int i = 0; i < eventColumns.size(); i++) {
                int len = getIntLE(data, pos);
                eventDataPos[i] = pos + 4;
                eventDataLen[i] = len;
                pos += 4 + len;
            }
        } else {
            this.seriesOffsets = new int[0];
            this.eventOffsets = new int[0];
            this.seriesCrcs = new int[0];
            this.eventCrcs = new int[0];
            this.seriesDataPos = new int[0];
            this.seriesDataLen = new int[0];
            this.eventDataPos = new int[0];
            this.eventDataLen = new int[0];
        }
    }

    private static final int READ_BUFFER_SIZE = 32 * 1024; // 32 KB

    /**
     * Read a Lastra file from an InputStream using a 32 KB read buffer.
     * Prefer {@link #from(byte[])} or {@link #from(java.nio.ByteBuffer)} when
     * the data is already in memory to avoid copying.
     */
    public static LastraReader from(InputStream in) throws IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        byte[] readBuf = new byte[READ_BUFFER_SIZE];
        int n;
        while ((n = in.read(readBuf)) != -1) {
            baos.write(readBuf, 0, n);
        }
        return new LastraReader(baos.toByteArray());
    }

    /**
     * Read a Lastra file from a byte array (zero-copy).
     */
    public static LastraReader from(byte[] data) {
        return new LastraReader(data);
    }

    /**
     * Read a Lastra file from a ByteBuffer (zero-copy if backed by array).
     */
    public static LastraReader from(java.nio.ByteBuffer buffer) {
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.remaining() == buffer.array().length) {
            return new LastraReader(buffer.array());
        }
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new LastraReader(data);
    }

    /**
     * Returns the footer size in bytes (from the footer size hint).
     * Useful for HTTP Range request planning: fetch last 8 bytes to get LAS! + footerSize,
     * then fetch footerSize bytes to parse row group stats before fetching data.
     *
     * @param data the last 8 bytes of the file
     * @return footer size, or -1 if not a valid Lastra trailer
     */
    public static int readFooterSize(byte[] trailer) {
        if (trailer.length < 8) return -1;
        int magic = getIntLE(trailer, trailer.length - 8);
        if (magic != Lastra.FOOTER_MAGIC) return -1;
        return getIntLE(trailer, trailer.length - 4);
    }

    public int seriesRowCount() { return seriesRowCount; }
    public int eventsRowCount() { return eventsRowCount; }
    public List<ColumnDescriptor> seriesColumns() { return seriesColumns; }
    public List<ColumnDescriptor> eventColumns() { return eventColumns; }

    public ColumnDescriptor getSeriesColumn(String name) {
        return findColumn(seriesColumns, name);
    }

    public ColumnDescriptor getEventColumn(String name) {
        return findColumn(eventColumns, name);
    }

    /** Returns true if this file contains per-column CRC32 checksums. */
    public boolean hasChecksums() { return hasChecksums; }

    // --- Row group access ---

    /** Number of row groups (1 for files without explicit row groups). */
    public int rowGroupCount() {
        return rowGroupStatsList.isEmpty() ? 1 : rowGroupStatsList.size();
    }

    /** Statistics for a specific row group. Returns null for single-RG files. */
    public RowGroupStats rowGroupStats(int rgIndex) {
        if (rgIndex < 0 || rgIndex >= rowGroupStatsList.size()) return null;
        return rowGroupStatsList.get(rgIndex);
    }

    /** All row group statistics. Empty list for single-RG files. */
    public List<RowGroupStats> rowGroupStats() {
        return Collections.unmodifiableList(rowGroupStatsList);
    }

    /** Read a LONG column from a specific row group. */
    public long[] readRowGroupLong(int rgIndex, String name) {
        int colIdx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractRgColumn(rgIndex, colIdx, name);
        return decodeLongColumn(colData, rowGroupStatsList.get(rgIndex).rowCount(),
                seriesColumns.get(colIdx).codec());
    }

    /** Read a DOUBLE column from a specific row group. */
    public double[] readRowGroupDouble(int rgIndex, String name) {
        int colIdx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractRgColumn(rgIndex, colIdx, name);
        return decodeDoubleColumn(colData, rowGroupStatsList.get(rgIndex).rowCount(),
                seriesColumns.get(colIdx).codec());
    }

    /** Read a BINARY column from a specific row group. */
    public byte[][] readRowGroupBinary(int rgIndex, String name) {
        int colIdx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractRgColumn(rgIndex, colIdx, name);
        return decodeBinaryColumn(colData, rowGroupStatsList.get(rgIndex).rowCount());
    }

    private byte[] extractRgColumn(int rgIndex, int colIdx, String name) {
        int pos = rgColPos.get(rgIndex)[colIdx];
        int len = rgColLen.get(rgIndex)[colIdx];
        byte[] data = new byte[len];
        System.arraycopy(buf.array(), pos, data, 0, len);
        if (hasChecksums && rgIndex < rgColCrcs.size()) {
            int expected = rgColCrcs.get(rgIndex)[colIdx];
            CRC32 crc = new CRC32();
            crc.update(data);
            int actual = (int) crc.getValue();
            if (actual != expected) {
                throw new IllegalStateException(String.format(
                        "CRC32 mismatch on RG %d column '%s': expected 0x%08X, got 0x%08X",
                        rgIndex, name, expected, actual));
            }
        }
        return data;
    }

    // --- Series column readers (read all row groups concatenated) ---

    public long[] readSeriesLong(String name) {
        if (!rowGroupStatsList.isEmpty()) {
            long[] result = new long[seriesRowCount];
            int offset = 0;
            for (int rg = 0; rg < rowGroupStatsList.size(); rg++) {
                long[] chunk = readRowGroupLong(rg, name);
                System.arraycopy(chunk, 0, result, offset, chunk.length);
                offset += chunk.length;
            }
            return result;
        }
        int idx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractAndVerify(seriesDataPos[idx], seriesDataLen[idx], seriesCrcs, idx, name);
        return decodeLongColumn(colData, seriesRowCount, seriesColumns.get(idx).codec());
    }

    public double[] readSeriesDouble(String name) {
        if (!rowGroupStatsList.isEmpty()) {
            double[] result = new double[seriesRowCount];
            int offset = 0;
            for (int rg = 0; rg < rowGroupStatsList.size(); rg++) {
                double[] chunk = readRowGroupDouble(rg, name);
                System.arraycopy(chunk, 0, result, offset, chunk.length);
                offset += chunk.length;
            }
            return result;
        }
        int idx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractAndVerify(seriesDataPos[idx], seriesDataLen[idx], seriesCrcs, idx, name);
        return decodeDoubleColumn(colData, seriesRowCount, seriesColumns.get(idx).codec());
    }

    public byte[][] readSeriesBinary(String name) {
        if (!rowGroupStatsList.isEmpty()) {
            byte[][] result = new byte[seriesRowCount][];
            int offset = 0;
            for (int rg = 0; rg < rowGroupStatsList.size(); rg++) {
                byte[][] chunk = readRowGroupBinary(rg, name);
                System.arraycopy(chunk, 0, result, offset, chunk.length);
                offset += chunk.length;
            }
            return result;
        }
        int idx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractAndVerify(seriesDataPos[idx], seriesDataLen[idx], seriesCrcs, idx, name);
        return decodeBinaryColumn(colData, seriesRowCount);
    }

    // --- Event column readers ---

    public long[] readEventLong(String name) {
        int idx = findColumnIndex(eventColumns, name);
        byte[] colData = extractAndVerify(eventDataPos[idx], eventDataLen[idx], eventCrcs, idx, name);
        return decodeLongColumn(colData, eventsRowCount, eventColumns.get(idx).codec());
    }

    public double[] readEventDouble(String name) {
        int idx = findColumnIndex(eventColumns, name);
        byte[] colData = extractAndVerify(eventDataPos[idx], eventDataLen[idx], eventCrcs, idx, name);
        return decodeDoubleColumn(colData, eventsRowCount, eventColumns.get(idx).codec());
    }

    public byte[][] readEventBinary(String name) {
        int idx = findColumnIndex(eventColumns, name);
        byte[] colData = extractAndVerify(eventDataPos[idx], eventDataLen[idx], eventCrcs, idx, name);
        return decodeBinaryColumn(colData, eventsRowCount);
    }

    // --- Decoders ---

    private long[] decodeLongColumn(byte[] data, int count, Lastra.Codec codec) {
        switch (codec) {
            case DELTA_VARINT: return DeltaVarintCodec.decode(data, count);
            case RAW: return RawCodec.decodeLongs(data, count);
            default: throw new IllegalArgumentException("Unsupported codec for LONG: " + codec);
        }
    }

    private double[] decodeDoubleColumn(byte[] data, int count, Lastra.Codec codec) {
        switch (codec) {
            case ALP: return new AlpDecompressor().decompress(data);
            case GORILLA: return GorillaCodec.decode(data, count);
            case PONGO: return PongoCodec.decode(data, count);
            case RAW: return RawCodec.decodeDoubles(data, count);
            default: throw new IllegalArgumentException("Unsupported codec for DOUBLE: " + codec);
        }
    }

    private byte[][] decodeBinaryColumn(byte[] data, int count) {
        return VarlenCodec.decode(data, count);
    }

    // --- Helpers ---

    private byte[] extractAndVerify(int pos, int len, int[] crcs, int idx, String colName) {
        byte[] data = new byte[len];
        System.arraycopy(buf.array(), pos, data, 0, len);
        if (hasChecksums && idx < crcs.length) {
            CRC32 crc = new CRC32();
            crc.update(data);
            int actual = (int) crc.getValue();
            if (actual != crcs[idx]) {
                throw new IllegalStateException(String.format(
                        "CRC32 mismatch on column '%s': expected 0x%08X, got 0x%08X",
                        colName, crcs[idx], actual));
            }
        }
        return data;
    }

    private List<ColumnDescriptor> readColumnDescriptors(int count) {
        List<ColumnDescriptor> cols = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int codecId = buf.get() & 0xFF;
            int typeId = buf.get() & 0xFF;
            int colFlags = buf.get() & 0xFF;
            int nameLen = buf.get() & 0xFF;
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            Map<String, String> metadata = Collections.emptyMap();
            if ((colFlags & 0x02) != 0) {
                int metaLen = buf.getShort() & 0xFFFF;
                byte[] metaBytes = new byte[metaLen];
                buf.get(metaBytes);
                metadata = parseJsonMap(new String(metaBytes, StandardCharsets.UTF_8));
            }
            cols.add(new ColumnDescriptor(name, Lastra.DataType.fromId(typeId),
                    Lastra.Codec.fromId(codecId), metadata));
        }
        return cols;
    }

    private static int findColumnIndex(List<ColumnDescriptor> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) return i;
        }
        throw new IllegalArgumentException("Column not found: " + name);
    }

    private static ColumnDescriptor findColumn(List<ColumnDescriptor> columns, String name) {
        return columns.get(findColumnIndex(columns, name));
    }

    /**
     * Locate the row-groups footer in a file that was written without the
     * footer-size-hint trailer (lastra-java ≤ 0.8.0 / lastra-convert ≤ 0.12.0).
     *
     * <p>Strategy: walk groups of {@code seriesColCount} length-prefixed
     * columns forward. After every candidate RG boundary, hypothesise that
     * we're just past the data section and check whether the remaining bytes
     * (events + footer) add up to the expected size for the current
     * {@code rgCount}. Footer size is fully determined by
     * {@code rgCount, seriesColCount, eventColCount, hasChecksums}; events
     * are length-prefixed so their total size is also discoverable. The
     * first match is the answer.
     *
     * @return {@code [footerPos, rgCount]}
     */
    private static int[] scanRowGroupsForFooter(byte[] data, int dataOffset,
                                                 int seriesColCount, int eventColCount,
                                                 boolean hasChecksums, int trailerSize) {
        int trailerEnd = data.length - trailerSize;
        int scan = dataOffset;
        int rgCount = 0;

        while (true) {
            // Hypothesis: we have just finished `rgCount` row groups. Try to
            // consume `eventColCount` length-prefixed event columns; if their
            // bytes plus the implied footer size land exactly on trailerEnd,
            // this is the boundary.
            int eventEnd = scan;
            boolean eventsValid = (eventColCount == 0);
            if (eventColCount > 0) {
                eventsValid = true;
                for (int e = 0; e < eventColCount && eventsValid; e++) {
                    if (eventEnd + 4 > trailerEnd) { eventsValid = false; break; }
                    int len = getIntLE(data, eventEnd);
                    if (len < 0 || eventEnd + 4 + len > trailerEnd) {
                        eventsValid = false; break;
                    }
                    eventEnd += 4 + len;
                }
            }
            if (eventsValid && rgCount > 0) {
                int remaining = trailerEnd - eventEnd;
                int expectedFooter = footerSizeFor(rgCount, seriesColCount,
                        eventColCount, hasChecksums);
                if (remaining == expectedFooter) {
                    return new int[]{eventEnd, rgCount};
                }
            }

            if (scan >= trailerEnd) {
                throw new IllegalArgumentException(
                        "Could not locate row-groups footer: scan ran off the data section");
            }
            // Otherwise, try to consume another row group.
            int next = scan;
            for (int c = 0; c < seriesColCount; c++) {
                if (next + 4 > trailerEnd) {
                    throw new IllegalArgumentException(
                            "Truncated row-group data while scanning for footer");
                }
                int len = getIntLE(data, next);
                if (len < 0 || next + 4 + len > trailerEnd) {
                    throw new IllegalArgumentException(
                            "Invalid column length while scanning for footer at byte "
                                    + next + " (rgCount=" + rgCount + ")");
                }
                next += 4 + len;
            }
            scan = next;
            rgCount++;
            if (rgCount > 1_000_000) {
                throw new IllegalArgumentException("Row-group scan exceeded 1M groups");
            }
        }
    }

    /** Total bytes occupied by the row-groups footer for the given parameters. */
    private static int footerSizeFor(int rgCount, int seriesColCount,
                                      int eventColCount, boolean hasChecksums) {
        int rgStats = rgCount * 24;
        int rgCrcs = hasChecksums ? rgCount * seriesColCount * 4 : 0;
        int eventOffsetsBytes = eventColCount * 4;
        int eventCrcsBytes = hasChecksums ? eventColCount * 4 : 0;
        return 4 + rgStats + rgCrcs + eventOffsetsBytes + eventCrcsBytes;
    }

    private static int getIntLE(byte[] data, int pos) {
        return (data[pos] & 0xFF)
                | ((data[pos + 1] & 0xFF) << 8)
                | ((data[pos + 2] & 0xFF) << 16)
                | ((data[pos + 3] & 0xFF) << 24);
    }

    private static long getLongLE(byte[] data, int pos) {
        return (getIntLE(data, pos) & 0xFFFFFFFFL)
                | ((long) getIntLE(data, pos + 4) << 32);
    }

    private static Map<String, String> parseJsonMap(String json) {
        // Minimal JSON object parser for {"key":"value",...}
        Map<String, String> map = new LinkedHashMap<>();
        json = json.trim();
        if (json.startsWith("{")) json = json.substring(1);
        if (json.endsWith("}")) json = json.substring(0, json.length() - 1);
        if (json.isEmpty()) return map;
        String[] pairs = json.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split(":", 2);
            String key = kv[0].trim().replace("\"", "");
            String value = kv[1].trim().replace("\"", "");
            map.put(key, value);
        }
        return map;
    }
}

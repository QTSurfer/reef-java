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

        if (hasFooter && hasRowGroups) {
            // Parse row group footer from the end
            // Footer layout: [rgCount:4] [rgCount × (offset:4 + rows:4 + tsMin:8 + tsMax:8)]
            //                 [rgCount × seriesColCount × CRC:4]
            //                 [eventColCount × offset:4] [eventColCount × CRC:4]
            //                 [LAS!:4]

            // First, find rgCount: scan backwards from LAS! magic
            // We know: last 4 bytes = LAS!, before that = event CRCs + event offsets
            // Before events = RG CRCs, before that = RG stats, before that = rgCount
            // Easier: scan forward from data to find where RGs end, then parse footer

            // Use a two-pass approach: first scan data forward to find all RG data positions,
            // then parse footer from end.

            // Actually, read rgCount from footer start. Footer size depends on rgCount which
            // we don't know yet. Read it by scanning from the end.
            int endPos = data.length - 4; // skip LAS!
            int footerMagic = getIntLE(data, endPos);
            if (footerMagic != Lastra.FOOTER_MAGIC) {
                throw new IllegalArgumentException("Invalid Lastra footer");
            }

            // Event CRCs + offsets (from end, before LAS!)
            int eventCols = eventColumns.size();
            int eventFooterSize = eventCols * 4 * (hasChecksums ? 2 : 1); // offsets + crcs
            int eventFooterStart = endPos - eventFooterSize;

            this.eventCrcs = new int[eventCols];
            this.eventOffsets = new int[eventCols];
            // Read event offsets then CRCs (or just offsets if no checksums)
            // They're written as: [event offsets][event CRCs]
            int ep = eventFooterStart;
            for (int i = 0; i < eventCols; i++) { eventOffsets[i] = getIntLE(data, ep); ep += 4; }
            if (hasChecksums) {
                for (int i = 0; i < eventCols; i++) { eventCrcs[i] = getIntLE(data, ep); ep += 4; }
            }

            // Before events: RG CRCs [rgCount × seriesColCount × 4]
            // Before that: RG stats [rgCount × 24 bytes]
            // Before that: rgCount [4 bytes]
            // We need to find rgCount first. It's at a known position relative to the end.
            // Total footer = rgCount(4) + rgCount×24 + rgCount×seriesColCount×4(crcs) + eventFooter + 4(magic)
            // Can't solve without rgCount. Read it from the data section boundary instead.

            // Scan data forward to count RG boundaries, or read rgCount from the footer.
            // The rgCount is the first int after the data+events section.
            // Simpler: parse data section forward, count RGs by scanning column length prefixes.

            // Actually, let's compute: we know the footer starts right after events data.
            // Scan series data + events data forward, then the first int is rgCount.
            // But we don't know where data ends without rgCount...

            // Simplest: scan backwards. rgCount × (24 + seriesColCount×4) + 4 + eventFooter + 4 = ???
            // We can try reading rgCount at different positions. But cleaner: read it from a known
            // location. Let me restructure: put rgCount as the LAST int before LAS! magic.
            // No — that changes the writer. Let me just read forward.

            // Forward approach: skip all RG data, then read footer sequentially.
            int scanPos = dataOffset;
            // We don't know how many RGs, but we can read until we hit the footer region.
            // The footer region starts after all data. We can detect it because the first
            // int of footer is rgCount (a small number like 1-100), vs column data lengths
            // (typically hundreds to thousands).

            // Actually the simplest correct approach: put rgCount at a FIXED position.
            // Read it from right before event footer:
            // [RG CRCs][rgCount:4][event offsets][event CRCs][LAS!]
            // No, that's messy. Let me just put rgCount right before LAS! along with
            // a flag. Or better: read the footer from end.

            // The writer writes: [rgCount][rg stats...][rg crcs...][event offsets][event crcs][LAS!]
            // From the end: LAS!(4) + eventCrcs(ec×4) + eventOffsets(ec×4) + rgCrcs(rc×sc×4) + rgStats(rc×24) + rgCount(4)
            // Total from end = 4 + ec×8 + rc×sc×4 + rc×24 + 4
            // We know ec and sc but not rc. Read rc from the position:
            // rgCountPos = endPos - eventFooterSize - ???
            // We can't compute without rc.

            // Solution: put rgCount at the END of footer, just before LAS!
            // New footer: [rg stats][rg crcs][event offsets][event crcs][rgCount][LAS!]
            // Then: rgCountPos = endPos - 4, and we read it first.
            // This requires changing the writer. Let me do that.

            // For now, assume rgCount is the 4 bytes just before eventFooter+LAS!
            // Actually the writer writes rgCount FIRST in the footer. Let me reparse
            // by reading forward from where data ends.

            // Forward scan: read all RG data, then footer is what remains.
            // Each RG has seriesColCount columns, each with [4-byte len][data].
            // Read them all, grouping by seriesColCount.
            int rgDataScanPos = dataOffset;
            List<int[]> tempRgColPos = new ArrayList<>();
            List<int[]> tempRgColLen = new ArrayList<>();
            // Keep reading groups of seriesColCount columns until we can't
            while (true) {
                // Peek: is the next int a plausible column length or rgCount?
                if (rgDataScanPos + 4 > data.length) break;
                int peek = getIntLE(data, rgDataScanPos);
                // After all RG data, next comes event data or footer.
                // Heuristic: column lengths are > 0 and < 10MB; rgCount would be < 1000
                // Better: track how much data we've consumed vs total.
                // We know total series data spans from dataOffset to events start.
                // But we don't know events start without knowing RG count.

                // Safest: read exactly seriesColCount columns, check if we're still in data range.
                int[] colPos = new int[seriesColCount];
                int[] colLen = new int[seriesColCount];
                boolean valid = true;
                int tempPos = rgDataScanPos;
                for (int c = 0; c < seriesColCount; c++) {
                    if (tempPos + 4 > data.length) { valid = false; break; }
                    int len = getIntLE(data, tempPos);
                    if (len < 0 || tempPos + 4 + len > data.length) { valid = false; break; }
                    colPos[c] = tempPos + 4;
                    colLen[c] = len;
                    tempPos += 4 + len;
                }
                if (!valid) break;

                // Verify this is actual column data, not footer
                // The footer starts with rgCount (small int). If peek == tempRgColPos.size()+1
                // and we've read some RGs, this might be the footer.
                // More robust: check if after reading this "RG", the next bytes parse as another
                // RG or as footer. For now, trust the structure.
                tempRgColPos.add(colPos);
                tempRgColLen.add(colLen);
                rgDataScanPos = tempPos;

                // Safety: don't read more than a reasonable number of RGs
                if (tempRgColPos.size() > 100000) break;
            }

            // Now rgDataScanPos points to events data (if any) or footer.
            // Read events data
            this.eventDataPos = new int[eventCols];
            this.eventDataLen = new int[eventCols];
            for (int i = 0; i < eventCols; i++) {
                int len = getIntLE(data, rgDataScanPos);
                eventDataPos[i] = rgDataScanPos + 4;
                eventDataLen[i] = len;
                rgDataScanPos += 4 + len;
            }

            // Now parse footer: rgCount, rg stats, rg CRCs
            int fp = rgDataScanPos;
            int rgCount = getIntLE(data, fp); fp += 4;

            // Verify rgCount matches what we scanned
            if (rgCount != tempRgColPos.size()) {
                throw new IllegalArgumentException("Row group count mismatch: footer=" + rgCount
                    + " scanned=" + tempRgColPos.size());
            }

            for (int i = 0; i < rgCount; i++) {
                int rgOffset = getIntLE(data, fp); fp += 4;
                int rgRows = getIntLE(data, fp); fp += 4;
                long rgTsMin = getLongLE(data, fp); fp += 8;
                long rgTsMax = getLongLE(data, fp); fp += 8;
                rowGroupStatsList.add(new RowGroupStats(rgRows, rgOffset, rgTsMin, rgTsMax));
            }

            // RG CRCs
            for (int i = 0; i < rgCount; i++) {
                int[] crcs = new int[seriesColCount];
                for (int c = 0; c < seriesColCount; c++) {
                    crcs[c] = getIntLE(data, fp); fp += 4;
                }
                rgColCrcs.add(crcs);
            }

            this.rgColPos.addAll(tempRgColPos);
            this.rgColLen.addAll(tempRgColLen);

            // Series-level pos/len not used with row groups, but set for compatibility
            this.seriesDataPos = new int[0];
            this.seriesDataLen = new int[0];
            this.seriesOffsets = new int[0];
            this.seriesCrcs = new int[0];

        } else if (hasFooter) {
            // No row groups: original flat format
            int totalCols = seriesColCount + eventColumns.size();
            int footerInts = totalCols; // offsets
            if (hasChecksums) footerInts += totalCols; // + CRCs
            footerInts += 1; // LAS! magic

            int footerStart = data.length - footerInts * 4;
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

            int footerMagic = footer.getInt();
            if (footerMagic != Lastra.FOOTER_MAGIC) {
                throw new IllegalArgumentException("Invalid Lastra footer");
            }

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

    /**
     * Read a Lastra file from an InputStream (loads entirely into memory).
     */
    public static LastraReader from(InputStream in) throws IOException {
        return new LastraReader(in.readAllBytes());
    }

    /**
     * Read a Lastra file from a byte array.
     */
    public static LastraReader from(byte[] data) {
        return new LastraReader(data);
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

package com.wualabs.qtsurfer.lastra;

/**
 * A single row group from a Lastra file, exposed by {@link LastraReader#readRowGroups()}.
 *
 * <p>Columns are decoded lazily on first access — heap residency stays bounded to one
 * row group's worth of decompressed primitive arrays plus the codec scratch state. The
 * iterator that produced this row group will release its references after the consumer
 * advances, so callers should not retain row groups across iterator steps.
 *
 * <p>Column indices match {@link LastraReader#seriesColumns()} order. Type mismatch
 * (e.g. {@link #getLongColumn(int)} on a DOUBLE column) throws
 * {@link IllegalArgumentException}.
 */
public final class RowGroup {

    /** Identifies the column type to dispatch the right decoder. */
    private static final int KIND_LONG = 0;
    private static final int KIND_DOUBLE = 1;
    private static final int KIND_BINARY = 2;

    private final int rgIndex;
    private final int rowCount;
    private final long tsMin;
    private final long tsMax;

    // Lazy column cache. Holds the decoded primitive array per column once requested.
    // Cleared by the iterator when it advances to the next row group (clear()).
    private final Object[] decoded;
    private final int[] kinds;
    private final LastraReader reader;

    RowGroup(LastraReader reader, int rgIndex, int rowCount, long tsMin, long tsMax,
             int columnCount) {
        this.reader = reader;
        this.rgIndex = rgIndex;
        this.rowCount = rowCount;
        this.tsMin = tsMin;
        this.tsMax = tsMax;
        this.decoded = new Object[columnCount];
        this.kinds = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            switch (reader.seriesColumns().get(i).dataType()) {
                case LONG: kinds[i] = KIND_LONG; break;
                case DOUBLE: kinds[i] = KIND_DOUBLE; break;
                case BINARY: kinds[i] = KIND_BINARY; break;
                default: throw new IllegalStateException(
                        "Unknown data type for column index " + i);
            }
        }
    }

    /** Number of rows in this row group. */
    public int rowCount() { return rowCount; }

    /** Minimum timestamp in this row group (from the footer stats, before decode). */
    public long tsMin() { return tsMin; }

    /** Maximum timestamp in this row group (from the footer stats, before decode). */
    public long tsMax() { return tsMax; }

    /** Index of this row group in the file (0-based). */
    public int index() { return rgIndex; }

    /**
     * Decode (or return cached) a LONG column from this row group.
     * @throws IllegalArgumentException if the column at {@code colIdx} is not LONG.
     */
    public long[] getLongColumn(int colIdx) {
        ensureKind(colIdx, KIND_LONG, "LONG");
        Object cached = decoded[colIdx];
        if (cached != null) return (long[]) cached;
        long[] arr = reader.decodeRowGroupLong(rgIndex, colIdx);
        decoded[colIdx] = arr;
        return arr;
    }

    /**
     * Decode (or return cached) a DOUBLE column from this row group.
     * @throws IllegalArgumentException if the column at {@code colIdx} is not DOUBLE.
     */
    public double[] getDoubleColumn(int colIdx) {
        ensureKind(colIdx, KIND_DOUBLE, "DOUBLE");
        Object cached = decoded[colIdx];
        if (cached != null) return (double[]) cached;
        double[] arr = reader.decodeRowGroupDouble(rgIndex, colIdx);
        decoded[colIdx] = arr;
        return arr;
    }

    /**
     * Decode (or return cached) a BINARY column from this row group.
     * @throws IllegalArgumentException if the column at {@code colIdx} is not BINARY.
     */
    public byte[][] getBinaryColumn(int colIdx) {
        ensureKind(colIdx, KIND_BINARY, "BINARY");
        Object cached = decoded[colIdx];
        if (cached != null) return (byte[][]) cached;
        byte[][] arr = reader.decodeRowGroupBinary(rgIndex, colIdx);
        decoded[colIdx] = arr;
        return arr;
    }

    /**
     * Drop all decoded column arrays so they can be GC'd. The iterator calls this
     * after the consumer advances past this row group.
     */
    void clear() {
        for (int i = 0; i < decoded.length; i++) decoded[i] = null;
    }

    private void ensureKind(int colIdx, int expected, String label) {
        if (kinds[colIdx] != expected) {
            throw new IllegalArgumentException(
                    "Column at index " + colIdx + " is not " + label
                            + " (it is " + reader.seriesColumns().get(colIdx).dataType() + ")");
        }
    }
}

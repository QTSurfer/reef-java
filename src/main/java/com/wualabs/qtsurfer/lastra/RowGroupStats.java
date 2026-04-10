package com.wualabs.qtsurfer.lastra;

/**
 * Statistics for a single row group in a Lastra file.
 *
 * <p>Used for predicate pushdown: a reader can skip row groups whose timestamp range
 * does not overlap the query window, avoiding unnecessary HTTP range requests and decoding.
 */
public final class RowGroupStats {

    private final int rowCount;
    private final int offset;
    private final long tsMin;
    private final long tsMax;

    public RowGroupStats(int rowCount, int offset, long tsMin, long tsMax) {
        this.rowCount = rowCount;
        this.offset = offset;
        this.tsMin = tsMin;
        this.tsMax = tsMax;
    }

    public int rowCount() { return rowCount; }
    public int offset() { return offset; }
    public long tsMin() { return tsMin; }
    public long tsMax() { return tsMax; }
}

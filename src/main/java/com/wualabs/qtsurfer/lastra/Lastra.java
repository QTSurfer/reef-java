package com.wualabs.qtsurfer.lastra;

/**
 * Lastra: columnar time series file format.
 *
 * <p>Constants and enumerations for the Lastra wire format.
 */
public final class Lastra {

    private Lastra() {}

    /** Magic bytes identifying a Lastra file. */
    public static final int MAGIC = 0x4C415354; // "LAST" in ASCII

    /** Footer sentinel. */
    public static final int FOOTER_MAGIC = 0x4C415321; // "LAS!" in ASCII

    /** Current format version. */
    public static final int VERSION = 1;

    /** File extension (without dot). */
    public static final String EXTENSION = "lastra";

    /** Column data types. */
    public enum DataType {
        LONG(0),
        DOUBLE(1),
        BINARY(2);

        public final int id;

        DataType(int id) { this.id = id; }

        public static DataType fromId(int id) {
            for (DataType t : values()) {
                if (t.id == id) return t;
            }
            throw new IllegalArgumentException("Unknown DataType id: " + id);
        }
    }

    /** Column compression codecs. */
    public enum Codec {
        /** No compression. */
        RAW(0),
        /** Delta-of-delta + zigzag varint. Best for timestamps (long). */
        DELTA_VARINT(1),
        /** ALP: decimal scaling + FOR + bit-packing. Best for decimal doubles. */
        ALP(2),
        /** Variable-length encoding. For binary/string columns. */
        VARLEN(3),
        /** Variable-length + ZSTD block compression. For JSON/binary bulk data. */
        VARLEN_ZSTD(4),
        /** Variable-length + gzip block compression. For metadata/small text. */
        VARLEN_GZIP(5),
        /** Gorilla XOR compression. For double columns. */
        GORILLA(6),
        /** Decimal-aware erasure + Gorilla XOR. Best for decimal-native doubles (prices). */
        PONGO(7);

        public final int id;

        Codec(int id) { this.id = id; }

        public static Codec fromId(int id) {
            for (Codec c : values()) {
                if (c.id == id) return c;
            }
            throw new IllegalArgumentException("Unknown Codec id: " + id);
        }
    }

    /** Header flags. */
    public static final int FLAG_HAS_EVENTS = 1;
    public static final int FLAG_HAS_FOOTER = 1 << 1;
    /** Per-column CRC32 checksums stored in footer. */
    public static final int FLAG_HAS_CHECKSUMS = 1 << 2;
    /** Multiple row groups with per-group statistics for range queries. */
    public static final int FLAG_HAS_ROW_GROUPS = 1 << 3;
}

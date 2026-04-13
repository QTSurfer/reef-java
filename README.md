<p align="center">
  <img src="logo.svg" alt="Lastra" width="420">
</p>

<p align="center">
  <a href="https://github.com/QTSurfer/lastra-java/actions/workflows/ci.yml"><img src="https://github.com/QTSurfer/lastra-java/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://jitpack.io/#QTSurfer/lastra-java"><img src="https://jitpack.io/v/QTSurfer/lastra-java.svg" alt="JitPack"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
</p>

<p align="center">
  Columnar time series file format optimized for numeric data.<br>
  Ideal for financial tick data, IoT sensors, and infrastructure metrics.
</p>

---

Combines [ALP](https://github.com/QTSurfer/alp-java), Gorilla, and Pongo compression for doubles, delta-varint for timestamps, and ZSTD/gzip for binary data — with per-column codec selection in a single `.lastra` file.

## Features

- **Per-column codecs**: ALP/Gorilla/Pongo for numeric data, delta-varint for timestamps, ZSTD/gzip for binary
- **Two sections**: regular time series (series) + sparse timestamped events (events)
- **Column metadata**: optional key-value metadata per column (e.g., indicator parameters, sensor config)
- **Selective column access**: footer offsets enable reading specific columns without decompressing others
- **Little-endian throughout**: JS/TS readers can use `Float64Array` zero-copy on decoded data
- **Zero Hadoop/Parquet dependency**: pure Java + [alp-java](https://github.com/QTSurfer/alp-java) + zstd-jni, Gorilla and Pongo codecs have zero external deps
- Java 11+

## File Format

```
HEADER (22 bytes, LE):
  "LAST" magic | version (1) | flags | seriesRowCount | seriesColCount
  eventsRowCount | eventsColCount

COLUMN DESCRIPTORS (series, then events):
  codec | dataType | flags | name
  optional: metadata (JSON, gzip-compressed)

SERIES DATA:
  Without row groups: per column: [4 bytes length] [compressed data]
  With row groups:    per RG: per column: [4 bytes length] [compressed data]

EVENTS DATA:        per column: [4 bytes length] [compressed data]

FOOTER:
  Without row groups: [column offsets] + [column CRC32s] + "LAS!" magic
  With row groups:    [rgCount] + [per-RG stats] + [per-RG CRC32s]
                      + [event offsets] + [event CRC32s] + "LAS!" magic
```

### Events section

All event columns share a single `eventsRowCount`. When columns have
different logical lengths, use the highest count and pad shorter columns
with zero/empty values. Use a `type` column (VARLEN) to identify event
categories and filter on read.

### Header flags

| Flag | Bit | Description |
|------|-----|-------------|
| `FLAG_HAS_EVENTS` | 0 | File contains an events section |
| `FLAG_HAS_FOOTER` | 1 | Footer with column offsets is present |
| `FLAG_HAS_CHECKSUMS` | 2 | Per-column CRC32 checksums in footer |
| `FLAG_HAS_ROW_GROUPS` | 3 | Multiple row groups with per-group statistics |

### Row groups

When `FLAG_HAS_ROW_GROUPS` is set, series data is partitioned into row groups. Each row group contains all series columns for a subset of rows, with codecs reset per group (enabling independent decoding).

The footer stores per-RG metadata: byte offset, row count, tsMin, tsMax. Readers can skip row groups whose timestamp range doesn't overlap the query window — enabling efficient HTTP range requests against remote files.

Row group size is configurable via `setRowGroupSize()` (default: 4096 rows). Files with fewer rows than the group size are written as a single implicit row group without the flag.

### Integrity: per-column CRC32

When `FLAG_HAS_CHECKSUMS` is set, the footer contains one CRC32 (IEEE 802.3) per column per row group, computed over the compressed data bytes. The reader verifies each column on access — a corrupted column throws an exception identifying which column failed, while intact columns remain readable.

## Codecs

| Codec | ID | DataType | Use case |
|-------|-----|----------|----------|
| `DELTA_VARINT` | 1 | LONG | Timestamps (~1 byte/value for regular intervals) |
| `ALP` | 2 | DOUBLE | Decimal doubles: prices, temperatures, measurements (~3-4 bits/value for 2dp) |
| `GORILLA` | 6 | DOUBLE | XOR compression ([Facebook VLDB 2015](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)). Best for volatile metrics: CPU%, latency, network |
| `PONGO` | 7 | DOUBLE | Decimal-aware erasure + Gorilla XOR. Best for decimal-native data: prices, sensor readings (~18 bits/value on 2dp) |
| `VARLEN` | 3 | BINARY | Short strings, event types, labels |
| `VARLEN_ZSTD` | 4 | BINARY | JSON payloads, bulk binary data |
| `VARLEN_GZIP` | 5 | BINARY | Metadata, small text (browser DecompressionStream compatible) |
| `RAW` | 0 | LONG/DOUBLE | Uncompressed fallback |

## Usage

### Write OHLCV ticker data

```java
try (LastraWriter w = new LastraWriter(outputStream)) {
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("open", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("high", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("low", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("volume", DataType.DOUBLE, Codec.ALP);
    w.writeSeries(rowCount, ts, open, high, low, close, volume);
}
```

### Write IoT sensor data with alerts

```java
try (LastraWriter w = new LastraWriter(outputStream)) {
    // Series: sensor readings with metadata
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("temperature", DataType.DOUBLE, Codec.PONGO,
        Map.of("unit", "celsius", "sensor", "dht22"));
    w.addSeriesColumn("humidity", DataType.DOUBLE, Codec.ALP,
        Map.of("unit", "%", "sensor", "dht22"));
    w.addSeriesColumn("pressure", DataType.DOUBLE, Codec.GORILLA);

    // Events: alerts with their own timestamps
    w.addEventColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addEventColumn("type", DataType.BINARY, Codec.VARLEN);
    w.addEventColumn("data", DataType.BINARY, Codec.VARLEN_ZSTD);

    w.writeSeries(sampleCount, ts, temp, humidity, pressure);
    w.writeEvents(alertCount, alertTs, alertTypes, alertData);
}
```

### Write financial strategy results

```java
try (LastraWriter w = new LastraWriter(outputStream)) {
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("ema1", DataType.DOUBLE, Codec.ALP,
        Map.of("indicator", "ema", "periods", "10"));
    w.addSeriesColumn("rsi1", DataType.DOUBLE, Codec.ALP,
        Map.of("indicator", "rsi", "periods", "14"));

    w.addEventColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addEventColumn("type", DataType.BINARY, Codec.VARLEN);
    w.addEventColumn("data", DataType.BINARY, Codec.VARLEN_ZSTD);

    w.writeSeries(tickCount, ts, close, ema, rsi);
    w.writeEvents(signalCount, signalTs, signalTypes, signalData);
}
```

### Write with row groups (for range queries)

```java
try (LastraWriter w = new LastraWriter(outputStream)) {
    w.setRowGroupSize(3600);  // 1 hour of 1s ticks per row group
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
    // writeSeries auto-partitions into row groups
    w.writeSeries(86400, dayTimestamps, dayCloses);  // 24 RGs for a full day
}
```

### Read (selective columns)

```java
LastraReader r = LastraReader.from(inputStream);

// Read only what you need — other columns are not decompressed
long[] ts = r.readSeriesLong("ts");
double[] close = r.readSeriesDouble("close");

// Column metadata
Map<String, String> meta = r.getSeriesColumn("ema1").metadata();
// {"indicator": "ema", "periods": "10"}

// Events (independent timestamps)
long[] signalTs = r.readEventLong("ts");
byte[][] signalData = r.readEventBinary("data");
```

### Read with temporal filtering (row groups)

```java
LastraReader r = LastraReader.from(inputStream);

// Skip row groups outside the query window
long queryFrom = Instant.parse("2026-04-07T14:00:00Z").toEpochMilli();
long queryTo   = Instant.parse("2026-04-07T16:00:00Z").toEpochMilli();

for (int i = 0; i < r.rowGroupCount(); i++) {
    RowGroupStats stats = r.rowGroupStats(i);
    if (stats.tsMax() < queryFrom || stats.tsMin() > queryTo) continue;

    // Only decode row groups that overlap the query range
    long[] ts = r.readRowGroupLong(i, "ts");
    double[] close = r.readRowGroupDouble(i, "close");
}
```

## Compression Ratios

OHLCV ticker data (1000 rows, 2 decimal places):

| Format | Size | Ratio |
|--------|------|-------|
| Raw | 48 KB | 1x |
| **Lastra** | **~3.5 KB** | **~13x** |
| [Apache Parquet](https://parquet.apache.org/) (SNAPPY) | ~8 KB | ~6x |

## Lastra vs Apache Parquet

### Codec comparison

| Aspect | [Apache Parquet](https://parquet.apache.org/) | Lastra |
|--------|---|---|
| **Timestamps (int64)** | [DELTA_BINARY_PACKED](https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-encoding-delta_binary_packed--5) (~1-2 bytes/value) | DELTA_VARINT (~1 byte/value) |
| **Doubles (numeric)** | PLAIN + block compression (SNAPPY/ZSTD) | **ALP** (~3-4 bits/value), **Pongo** (~18 bits/value), **Gorilla** (XOR) |
| **Strings / binary** | DICTIONARY + RLE, DELTA_BYTE_ARRAY | VARLEN, VARLEN_ZSTD, VARLEN_GZIP |
| **Block compression** | SNAPPY, GZIP, ZSTD, LZ4, BROTLI | No (compression integrated per codec) |
| **Per-column codec** | Same codec per file or row group | **Different codec per column** |
| **Optimized for** | General purpose, big data | Numeric time series (financial, IoT, infra) |

### Real-world benchmark (BTC/USDT, 3,591 rows, 11 columns)

| Format | Size | Ratio vs Parquet |
|--------|------|------------------|
| [Apache Parquet](https://parquet.apache.org/) (SNAPPY) | 118 KB | 1x |
| Lastra (ALP default) | 82 KB | 1.4x smaller |
| **Lastra (mixed codecs via [lastra-convert](https://github.com/QTSurfer/qtsurfer-lastra-convert) `--best`)** | **73 KB** | **1.6x smaller** |

### Why Lastra compresses better for numeric time series

[Apache Parquet](https://parquet.apache.org/) stores doubles as raw 8 bytes (PLAIN encoding) then applies generic block compression (SNAPPY/ZSTD). Lastra applies **semantic compression** per column:

- **ALP**: understands that `65007.28` has 2 decimal places → 3-4 bits/value
- **Pongo**: detects decimal patterns and erases mantissa noise before XOR → ~18 bits/value
- **Gorilla**: XOR between consecutive similar values → good for volatile data

### Where Parquet wins

[Apache Parquet](https://parquet.apache.org/) has a much larger ecosystem (Spark, DuckDB, Arrow, Pandas) and advanced features: predicate pushdown, bloom filters, column statistics, nested types, and modular encryption.

## Lastra vs Apache ORC

### Codec comparison

| Aspect | [Apache ORC](https://orc.apache.org/) | Lastra |
|--------|---|---|
| **Timestamps (int64)** | RLE + delta encoding (~1-2 bytes/value) | DELTA_VARINT (~1 byte/value) |
| **Doubles (numeric)** | **PLAIN (raw 8 bytes)** + block compression (ZLIB/ZSTD) | **ALP** (~3-4 bits/value), **Pongo** (~18 bits/value), **Gorilla** (XOR) |
| **Strings / binary** | DICTIONARY + RLE, DIRECT | VARLEN, VARLEN_ZSTD, VARLEN_GZIP |
| **Block compression** | ZLIB, ZSTD, Snappy, LZ4 | No (compression integrated per codec) |
| **Per-column codec** | Same codec for all columns | **Different codec per column** |
| **Row groups** | Stripes (250 MB), row groups (10K rows) | Configurable (default 4096 rows) |
| **Predicate pushdown** | Min/max per stripe + bloom filters | tsMin/tsMax per row group |
| **ACID transactions** | Yes (Hive) | No |
| **Schema** | Protobuf in footer (nested types, unions) | Fixed 22-byte header (flat types) |
| **Dependencies** | Hadoop, Protobuf, hive-storage-api | alp-java + zstd-jni |
| **Optimized for** | Data warehousing, ACID, big data | Numeric time series (financial, IoT, infra) |

### Why Lastra compresses doubles better than ORC

[Apache ORC](https://orc.apache.org/) encodes doubles as **raw 8 bytes** (IEEE 754 PLAIN) then applies generic block compression (ZLIB/ZSTD) over the entire stripe. This is the same limitation as Parquet — generic compressors don't understand decimal structure.

Lastra applies **semantic compression** that understands the data:
- **ALP** knows `65007.28` has 2 decimal places → encodes as integer `6500728` → 3-4 bits/value
- **Pongo** erases mantissa noise before XOR → ~18 bits/value
- Result: **~2x better compression** than ORC for numeric time series

### Where ORC wins

[Apache ORC](https://orc.apache.org/) has ACID transaction support (Hive), bloom filters for equality predicates, excellent integer encoding (RLE v2), a mature ecosystem (Hive, Spark, Presto/Trino, Flink, Iceberg), and support for complex nested types via Protobuf schemas.

## Lastra vs ClickHouse Native Format

ClickHouse is the closest comparison because it also supports **per-column codec selection** for doubles — unlike Parquet and ORC which use PLAIN encoding.

### Codec comparison

| Aspect | [ClickHouse](https://clickhouse.com/) native | Lastra |
|--------|---|---|
| **Timestamps** | DoubleDelta + LZ4 (~1-2 bytes/value) | DELTA_VARINT (~1 byte/value) |
| **Doubles (numeric)** | **Gorilla** (XOR), **FPC** (predictor-based) + ZSTD | **ALP** (~3-4 bits/value), **Pongo** (~18 bits/value), **Gorilla** (XOR) |
| **Per-column codec** | Yes (`CODEC(Gorilla, ZSTD)` per column in DDL) | Yes (per column in file header) |
| **Block compression** | LZ4, LZ4HC, ZSTD (layered on top of codecs) | No (compression integrated per codec) |
| **Codec pipeline** | Codec first → block compression second | Single-pass per codec |
| **Row groups** | Granules (8192 rows default, configurable) | Configurable (default 4096 rows) |
| **ALP support** | Not yet ([under discussion](https://github.com/ClickHouse/ClickHouse/discussions/60393)) | **Yes** (via [alp-java](https://github.com/QTSurfer/alp-java)) |
| **Format type** | Database engine storage (`.bin` + `.mrk2` + `primary.idx`) | Standalone file (`.lastra`) |
| **Dependencies** | Full ClickHouse server | alp-java + zstd-jni |
| **Use case** | OLAP database with SQL queries | Portable file format for archival and exchange |

### The double compression gap

ClickHouse offers **Gorilla** and **FPC** for Float64 columns — both are XOR/predictor-based codecs that work well for volatile metrics but don't understand decimal structure.

For financial prices like `65007.28`:
- **ClickHouse Gorilla**: XOR between consecutive values → ~32-40 bits/value (depends on volatility)
- **ClickHouse FPC**: Predictor + XOR → similar range, faster decompression
- **Lastra ALP**: Decimal-aware → `65007.28` × 100 = `6500728` → integer encoding → **3-4 bits/value**
- **Lastra Pongo**: Decimal erasure + XOR → **~18 bits/value**

ALP is [published research](https://dl.acm.org/doi/10.1145/3626717) (ACM SIGMOD 2024) showing **50% better compression and 44x faster scans** than Gorilla. ClickHouse has it as an open feature request but hasn't integrated it yet.

### Where ClickHouse wins

ClickHouse is a full OLAP database — SQL queries, distributed joins, materialized views, real-time ingestion, replication, and a massive ecosystem. Lastra is a file format. If you need a query engine, use ClickHouse (and potentially store exports as `.lastra` files for archival or cross-system exchange).

## Dependency (JitPack)

### Maven

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.qtsurfer</groupId>
    <artifactId>lastra-java</artifactId>
    <version>0.8.0</version>
</dependency>
```

### Gradle

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.qtsurfer:lastra-java:0.8.0'
}
```

## License

Copyright 2026 Wualabs LTD. Apache License 2.0 — see [LICENSE](LICENSE).

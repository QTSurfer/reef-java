# Reef

[![CI](https://github.com/QTSurfer/reef-java/actions/workflows/ci.yml/badge.svg)](https://github.com/QTSurfer/reef-java/actions/workflows/ci.yml)
[![JitPack](https://jitpack.io/v/QTSurfer/reef-java.svg)](https://jitpack.io/#QTSurfer/reef-java)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Columnar time series file format with per-column codec selection.

Designed for financial tick data and strategy signals. Combines [ALP](https://github.com/QTSurfer/alp-java) compression for doubles, delta-varint for timestamps, and ZSTD/gzip for binary data into a single `.reef` file.

## Features

- **Per-column codecs**: ALP for prices/indicators, delta-varint for timestamps, ZSTD/gzip for JSON signals
- **Two sections**: regular time series (series) + sparse timestamped events (events)
- **Column metadata**: optional key-value metadata per column (e.g., indicator parameters)
- **Selective column access**: footer offsets enable reading specific columns without decompressing others
- **Little-endian throughout**: JS/TS readers can use `Float64Array` zero-copy on decoded data
- **Zero Hadoop/Parquet dependency**: pure Java + [alp-java](https://github.com/QTSurfer/alp-java) + zstd-jni
- Java 11+

## File Format

```
HEADER (24 bytes, LE):
  "REEF" magic | version (1) | flags | seriesRowCount | seriesColCount
  eventsRowCount | eventsColCount

COLUMN DESCRIPTORS (series, then events):
  codec | dataType | flags | name
  optional: metadata (JSON, gzip-compressed)

SERIES DATA:        per column: [4 bytes length] [compressed data]
EVENTS DATA:        per column: [4 bytes length] [compressed data]

FOOTER:             column offsets + "REF!" magic
```

## Codecs

| Codec | DataType | Use case |
|-------|----------|----------|
| `DELTA_VARINT` | LONG | Timestamps (~1 byte/value for regular intervals) |
| `ALP` | DOUBLE | Prices, indicators (~3-4 bits/value for 2dp financial data) |
| `VARLEN` | BINARY | Short strings, signal types |
| `VARLEN_ZSTD` | BINARY | JSON payloads, bulk binary data |
| `VARLEN_GZIP` | BINARY | Metadata, small text (browser DecompressionStream compatible) |
| `RAW` | LONG/DOUBLE | Uncompressed fallback |

## Usage

### Write OHLCV ticker data

```java
try (ReefWriter w = new ReefWriter(outputStream)) {
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("open", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("high", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("low", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("volume", DataType.DOUBLE, Codec.ALP);
    w.writeSeries(rowCount, ts, open, high, low, close, volume);
}
```

### Write strategy results with signals

```java
try (ReefWriter w = new ReefWriter(outputStream)) {
    // Series: indicators with metadata
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("ema1", DataType.DOUBLE, Codec.ALP,
        Map.of("indicator", "ema", "periods", "10"));
    w.addSeriesColumn("rsi1", DataType.DOUBLE, Codec.ALP,
        Map.of("indicator", "rsi", "periods", "14"));

    // Events: signals with their own timestamps
    w.addEventColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addEventColumn("type", DataType.BINARY, Codec.VARLEN);
    w.addEventColumn("data", DataType.BINARY, Codec.VARLEN_ZSTD);

    w.writeSeries(tickCount, ts, close, ema, rsi);
    w.writeEvents(signalCount, signalTs, signalTypes, signalData);
}
```

### Read (selective columns)

```java
ReefReader r = ReefReader.from(inputStream);

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

## Compression Ratios

OHLCV ticker data (1000 rows, 2 decimal places):

| Format | Size | Ratio |
|--------|------|-------|
| Raw | 48 KB | 1x |
| **Reef** | **~3.5 KB** | **~13x** |
| Parquet (SNAPPY) | ~8 KB | ~6x |

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
    <artifactId>reef-java</artifactId>
    <version>0.5.0</version>
</dependency>
```

### Gradle

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.qtsurfer:reef-java:0.5.0'
}
```

## License

Copyright 2026 Wualabs LTD. Apache License 2.0 — see [LICENSE](LICENSE).

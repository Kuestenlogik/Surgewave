# Parquet Connector

The Parquet connector enables reading from and writing to Apache Parquet columnar files.

## Overview

- **Source**: Read records from Parquet files with offset tracking
- **Sink**: Write records to Parquet files with compression and row group configuration

**Use Cases:**
- Big data analytics pipeline integration
- Data lake ingestion/export
- Batch processing workflows
- Analytics data interchange
- Columnar storage for efficient queries

## Quick Start

### Parquet Source

Read data from Parquet files:

```json
{
  "name": "parquet-source",
  "config": {
    "connector.class": "ParquetSourceConnector",
    "parquet.file.path": "/data/analytics.parquet",
    "parquet.topic": "parquet-data",
    "parquet.batch.size": "1000"
  }
}
```

### Parquet Sink

Write records to Parquet files:

```json
{
  "name": "parquet-sink",
  "config": {
    "connector.class": "ParquetSinkConnector",
    "topics": "analytics-events",
    "parquet.output.path": "/output/analytics.parquet",
    "parquet.compression.codec": "snappy",
    "parquet.output.mode": "overwrite"
  }
}
```

## Configuration Reference

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `parquet.file.path` | string | Required | Path to Parquet file(s). Supports `;` delimited list |
| `parquet.topic` | string | Required | Target topic for records |
| `parquet.batch.size` | int | `1000` | Number of rows to read per batch |
| `parquet.poll.interval.ms` | long | `1000` | Poll interval in milliseconds |
| `parquet.delete.after.read` | boolean | `false` | Delete file after processing |
| `parquet.move.after.read` | boolean | `false` | Move file after processing |
| `parquet.processed.directory` | string | - | Directory for processed files |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Comma-separated list of topics to consume |
| `parquet.output.path` | string | Required | Output file or directory path |
| `parquet.output.mode` | string | `append` | Output mode: `append`, `overwrite`, `rolling` |
| `parquet.max.records.per.file` | int | `0` | Max records per file (rolling mode, 0 = unlimited) |
| `parquet.file.name.pattern` | string | `${topic}-${timestamp}.parquet` | File name pattern for rolling mode |
| `parquet.compression.codec` | string | `gzip` | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd`, `brotli` |
| `parquet.row.group.size` | int | `5000` | Number of rows per row group |

## Compression Codecs

| Codec | Description | Best For |
|-------|-------------|----------|
| `none` | No compression | Maximum write speed |
| `gzip` | Good balance of speed and ratio | General use |
| `snappy` | Very fast with moderate ratio | Real-time analytics |
| `lz4` | Fastest decompression | Read-heavy workloads |
| `zstd` | High compression ratio | Storage optimization |
| `brotli` | Excellent compression | Archival storage |

## Output Modes

### Append Mode
Records are appended to an existing file by reading and merging with new data.

```json
{
  "parquet.output.mode": "append",
  "parquet.output.path": "/output/events.parquet"
}
```

### Overwrite Mode
File is recreated on each flush with only the current batch.

```json
{
  "parquet.output.mode": "overwrite",
  "parquet.output.path": "/output/events.parquet"
}
```

### Rolling Mode
Creates new files based on record count. Supports placeholders:
- `${topic}` - Topic name
- `${partition}` - Partition number
- `${timestamp}` - Current timestamp (yyyyMMddHHmmss)

```json
{
  "parquet.output.mode": "rolling",
  "parquet.output.path": "/output/",
  "parquet.max.records.per.file": "100000",
  "parquet.file.name.pattern": "events-${timestamp}.parquet"
}
```

## Data Format

### Source Records

Each Parquet row is converted to a JSON object with column names as keys:

**Input Parquet Schema:**
```
id: INT64
name: STRING
active: BOOLEAN
```

**Output Records:**
```json
{"id": "1", "name": "Alice", "active": "true"}
{"id": "2", "name": "Bob", "active": "false"}
```

### Sink Records

JSON records are flattened to Parquet columns. All values are stored as strings in the current implementation.

**Input Record:**
```json
{"id": 1, "name": "Alice", "active": true}
```

**Output Parquet:**
Columns: `id` (STRING), `name` (STRING), `active` (STRING)

## Record Headers

Source records include metadata headers:
- `parquet.file` - Source file path
- `parquet.row` - Row number in source file

## Offset Tracking

The Parquet source connector tracks progress using:
- File path
- Row index
- File modification timestamp

This allows resuming from the last read position after restarts.

## Row Groups

Parquet files are organized into row groups for efficient reads. Configure row group size based on your access patterns:

- **Smaller row groups** (1000-5000): Better for random access and filtering
- **Larger row groups** (50000+): Better for sequential scans and compression

```json
{
  "parquet.row.group.size": "10000"
}
```

## Examples

### Multi-file Processing

Process multiple Parquet files:

```json
{
  "name": "parquet-multi-source",
  "config": {
    "connector.class": "ParquetSourceConnector",
    "parquet.file.path": "/data/part1.parquet;/data/part2.parquet",
    "parquet.topic": "parquet-data",
    "parquet.delete.after.read": "true"
  }
}
```

### High-Performance Sink

Optimized configuration for high throughput:

```json
{
  "name": "high-perf-sink",
  "config": {
    "connector.class": "ParquetSinkConnector",
    "topics": "events",
    "parquet.output.path": "/output/events/",
    "parquet.output.mode": "rolling",
    "parquet.compression.codec": "snappy",
    "parquet.row.group.size": "50000",
    "parquet.max.records.per.file": "1000000"
  }
}
```

### Analytics Export

Export analytics data with high compression:

```json
{
  "name": "analytics-export",
  "config": {
    "connector.class": "ParquetSinkConnector",
    "topics": "user-analytics",
    "parquet.output.path": "/analytics/daily/",
    "parquet.output.mode": "rolling",
    "parquet.compression.codec": "zstd",
    "parquet.file.name.pattern": "analytics-${timestamp}.parquet"
  }
}
```

## Integration with Data Lakes

Parquet is the standard format for data lakes. Use this connector to:
- Ingest data from S3/Azure/GCS Parquet files
- Export streams to data lake storage
- Process analytics datasets
- Bridge streaming and batch workloads

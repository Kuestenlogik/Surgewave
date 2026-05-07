# CSV Connector

The CSV connector enables reading from and writing to CSV files with RFC 4180 compliance.

## Overview

- **Source**: Read records from CSV files with offset tracking
- **Sink**: Write records to CSV files with append, overwrite, or rolling modes

**Use Cases:**
- Data migration from CSV exports
- Log file ingestion
- Batch data processing
- Report generation
- Data interchange with legacy systems

## Quick Start

### CSV Source

Read data from CSV files:

```json
{
  "name": "csv-source",
  "config": {
    "connector.class": "CsvSourceConnector",
    "csv.file.path": "/data/users.csv",
    "csv.topic": "csv-data",
    "csv.has.header": "true",
    "csv.delimiter": ",",
    "csv.key.field": "id"
  }
}
```

### CSV Sink

Write records to CSV files:

```json
{
  "name": "csv-sink",
  "config": {
    "connector.class": "CsvSinkConnector",
    "topics": "user-events",
    "csv.output.path": "/output/events.csv",
    "csv.include.header": "true",
    "csv.output.mode": "append"
  }
}
```

## Configuration Reference

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `csv.file.path` | string | Required | Path to CSV file(s). Supports `;` delimited list |
| `csv.topic` | string | Required | Target topic for records |
| `csv.has.header` | boolean | `true` | Whether the CSV file has a header row |
| `csv.delimiter` | string | `,` | Field delimiter character |
| `csv.encoding` | string | `utf-8` | File encoding |
| `csv.key.field` | string | - | Header field to use as message key |
| `csv.trim.fields` | boolean | `false` | Trim whitespace from field values |
| `csv.ignore.blank.lines` | boolean | `true` | Skip empty lines |
| `csv.delete.after.read` | boolean | `false` | Delete file after processing |
| `csv.move.after.read` | boolean | `false` | Move file after processing |
| `csv.processed.directory` | string | - | Directory for processed files |
| `csv.poll.interval.ms` | long | `1000` | Poll interval in milliseconds |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Comma-separated list of topics to consume |
| `csv.output.path` | string | Required | Output file or directory path |
| `csv.include.header` | boolean | `true` | Include header row in output |
| `csv.delimiter` | string | `,` | Field delimiter character |
| `csv.encoding` | string | `utf-8` | File encoding |
| `csv.output.mode` | string | `append` | Output mode: `append`, `overwrite`, `rolling` |
| `csv.max.records.per.file` | int | `0` | Max records per file (rolling mode, 0 = unlimited) |
| `csv.file.name.pattern` | string | `${topic}-${timestamp}.csv` | File name pattern for rolling mode |

## Output Modes

### Append Mode
Records are appended to an existing file. Header is only written if the file is empty or new.

```json
{
  "csv.output.mode": "append",
  "csv.output.path": "/output/events.csv"
}
```

### Overwrite Mode
File is truncated on start. All records are written fresh.

```json
{
  "csv.output.mode": "overwrite",
  "csv.output.path": "/output/events.csv"
}
```

### Rolling Mode
Creates new files based on record count or pattern. Supports placeholders:
- `${topic}` - Topic name
- `${partition}` - Partition number
- `${timestamp}` - Current timestamp (yyyyMMddHHmmss)

```json
{
  "csv.output.mode": "rolling",
  "csv.output.path": "/output/",
  "csv.max.records.per.file": "10000",
  "csv.file.name.pattern": "${topic}-${timestamp}.csv"
}
```

## Data Format

### Source Records

Each CSV row is converted to a JSON object with header names as keys:

**Input CSV:**
```csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

**Output Records:**
```json
{"id": "1", "name": "Alice", "email": "alice@example.com"}
{"id": "2", "name": "Bob", "email": "bob@example.com"}
```

For CSV without headers, field names are auto-generated as `field_0`, `field_1`, etc.

### Sink Records

JSON records are flattened to CSV columns. Nested objects are serialized as JSON strings.

**Input Record:**
```json
{"id": 1, "name": "Alice", "active": true}
```

**Output CSV:**
```csv
id,name,active
1,Alice,true
```

## Record Headers

Source records include metadata headers:
- `csv.file` - Source file path
- `csv.line` - Line number in source file

## Offset Tracking

The CSV source connector tracks progress using:
- File path
- Line number
- File modification timestamp

This allows resuming from the last read position after restarts.

## Examples

### Multi-file Processing

Process multiple CSV files:

```json
{
  "name": "csv-multi-source",
  "config": {
    "connector.class": "CsvSourceConnector",
    "csv.file.path": "/data/file1.csv;/data/file2.csv;/data/file3.csv",
    "csv.topic": "csv-data",
    "csv.delete.after.read": "true"
  }
}
```

### Tab-Delimited Files

Handle TSV files:

```json
{
  "name": "tsv-source",
  "config": {
    "connector.class": "CsvSourceConnector",
    "csv.file.path": "/data/data.tsv",
    "csv.topic": "tsv-data",
    "csv.delimiter": "\\t"
  }
}
```

### High-Volume Rolling Output

Generate rolling files for high-volume streams:

```json
{
  "name": "high-volume-sink",
  "config": {
    "connector.class": "CsvSinkConnector",
    "topics": "metrics",
    "csv.output.path": "/output/metrics/",
    "csv.output.mode": "rolling",
    "csv.max.records.per.file": "100000",
    "csv.file.name.pattern": "metrics-${partition}-${timestamp}.csv"
  }
}
```

## RFC 4180 Compliance

The CSV connector follows RFC 4180 standards:
- Fields containing delimiters, quotes, or newlines are quoted
- Quote characters within fields are escaped by doubling
- CRLF line endings are handled correctly
- Optional header row support

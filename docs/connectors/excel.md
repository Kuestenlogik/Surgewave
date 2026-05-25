# Excel Connector

The Excel connector enables reading from and writing to Microsoft Excel files (.xlsx format) using the ClosedXML library.

## Overview

- **Source Connector**: Reads rows from Excel worksheets and produces records
- **Sink Connector**: Writes records to Excel files with configurable output modes

## Source Connector

### Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `excel.file.path` | string | (required) | Path to Excel file(s). Supports `;` delimited list |
| `excel.topic` | string | (required) | Target topic for records |
| `excel.sheet.name` | string | | Sheet name to read (optional) |
| `excel.sheet.index` | int | 1 | Sheet index to read (1-based, used if sheet name not specified) |
| `excel.has.header` | boolean | true | Whether the first row contains column headers |
| `excel.start.row` | int | 1 | Starting row number (1-based) |
| `excel.end.row` | int | 0 | Ending row number (0 = read to end) |
| `excel.start.column` | int | 1 | Starting column number (1-based) |
| `excel.end.column` | int | 0 | Ending column number (0 = read to end) |
| `excel.key.column` | string | | Column name or number to use as message key |
| `excel.batch.size` | int | 1000 | Number of rows to read per batch |
| `excel.poll.interval.ms` | long | 1000 | Poll interval in milliseconds |
| `excel.delete.after.read` | boolean | false | Delete file after processing |
| `excel.move.after.read` | boolean | false | Move file to processed directory after reading |
| `excel.processed.directory` | string | | Directory to move processed files to |

### Example Configuration

```json
{
  "name": "excel-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Excel.ExcelSourceConnector",
  "excel.file.path": "C:/data/input.xlsx",
  "excel.topic": "excel-data",
  "excel.sheet.name": "Sales",
  "excel.has.header": "true",
  "excel.key.column": "OrderId",
  "excel.batch.size": "500"
}
```

### Record Format

Each row is converted to a JSON object where column headers (or generated column names) become field names:

```json
{
  "OrderId": "12345",
  "Customer": "Acme Corp",
  "Amount": 1500.00,
  "Date": "2024-01-15T00:00:00"
}
```

### Headers

Source records include the following headers:
- `excel.file`: Source file path
- `excel.sheet`: Worksheet name
- `excel.row`: Row number (1-based)

## Sink Connector

### Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `topics` | string | (required) | Comma-separated list of topics to consume |
| `excel.output.path` | string | (required) | Output directory or file path for Excel files |
| `excel.output.mode` | string | overwrite | Output mode: `append`, `overwrite`, or `rolling` |
| `excel.output.sheet.name` | string | Sheet1 | Name of the worksheet to write to |
| `excel.include.header` | boolean | true | Include header row in output file |
| `excel.max.rows.per.file` | int | 0 | Maximum rows per file for rolling mode (0 = unlimited) |
| `excel.file.name.pattern` | string | ${topic}-${timestamp}.xlsx | File name pattern for rolling mode |

### Output Modes

- **overwrite**: Creates a new file, replacing any existing file
- **append**: Appends records to an existing file or creates a new one
- **rolling**: Creates new files when the row limit is reached

### File Name Pattern Variables

For rolling mode, the following variables can be used in the file name pattern:
- `${topic}`: Topic name
- `${timestamp}`: Current timestamp (yyyyMMddHHmmss)
- `${partition}`: Partition number

### Example Configuration

```json
{
  "name": "excel-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Excel.ExcelSinkConnector",
  "topics": "processed-data",
  "excel.output.path": "C:/data/output",
  "excel.output.mode": "rolling",
  "excel.output.sheet.name": "Results",
  "excel.max.rows.per.file": "10000",
  "excel.file.name.pattern": "export-${topic}-${timestamp}.xlsx"
}
```

## Data Type Handling

### Reading (Source)

| Excel Type | JSON Type |
|------------|-----------|
| Boolean | boolean |
| Number | number |
| DateTime | string (ISO 8601) |
| TimeSpan | string |
| Text | string |
| Empty | null |

### Writing (Sink)

The sink connector expects JSON records. Field values are mapped to Excel cell types:

| JSON Type | Excel Type |
|-----------|------------|
| boolean | Boolean |
| number (integer) | Number |
| number (decimal) | Number |
| string (date format) | DateTime |
| string | Text |
| null | Empty |

## Multiple Files

### Source

Multiple files can be specified using semicolon separators:

```
excel.file.path=file1.xlsx;file2.xlsx;file3.xlsx
```

Files are distributed across tasks for parallel processing.

### Sink

For rolling mode, multiple files are created automatically based on the row limit and file name pattern.

## Offset Management

The source connector tracks:
- Current file path
- Worksheet name
- Row index
- File modification timestamp

This enables resuming from the last processed position after a restart.

## Error Handling

- Missing files are skipped during source processing
- Invalid Excel files are skipped with error logging
- Non-JSON sink records are handled by wrapping raw values in a single-column format

## Dependencies

- ClosedXML for Excel file operations
- System.Text.Json for JSON serialization

## Limitations

- Only `.xlsx` format is supported (not legacy `.xls`)
- Large files may impact memory usage
- Date/time values are converted to ISO 8601 strings when reading

# SFTP Connector

The SFTP connector provides integration with SFTP servers for downloading files from remote directories and uploading files to remote locations.

## Package

```
Kuestenlogik.Surgewave.Connect.Sftp
```

## Features

### Source Connector
- Poll remote directories for new/modified files
- Configurable file pattern filtering (glob syntax)
- Recursive directory traversal
- Delete or move files after reading
- Include file metadata as JSON with base64 content
- Start from latest (new files) or earliest (all files)
- SSH key and password authentication
- Configurable file size limits

### Sink Connector
- Upload files to remote SFTP directories
- Write or append mode for output
- Automatic directory creation
- Temporary file suffix during upload (atomic writes)
- Path templates with dynamic variables
- Overwrite protection option

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `topic` | String | Destination topic for file events |
| `sftp.host` | String | SFTP server hostname |
| `sftp.username` | String | SSH username |

### Authentication (one required)

| Setting | Type | Description |
|---------|------|-------------|
| `sftp.password` | Password | SSH password |
| `sftp.private.key.path` | String | Path to SSH private key file |
| `sftp.private.key.content` | String | SSH private key as string |
| `sftp.private.key.passphrase` | Password | Passphrase for encrypted private key |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `sftp.port` | Int | `22` | SFTP server port |
| `sftp.timeout.seconds` | Int | `30` | Connection and operation timeout |
| `sftp.remote.path` | String | `/` | Remote directory to watch |
| `sftp.file.pattern` | String | `*` | Glob pattern for matching files |
| `sftp.recursive` | Boolean | `false` | Recursively scan subdirectories |
| `sftp.poll.interval.ms` | Int | `30000` | Poll interval in milliseconds |
| `sftp.delete.after.read` | Boolean | `false` | Delete files after processing |
| `sftp.move.after.read` | Boolean | `false` | Move files after processing |
| `sftp.move.to.path` | String | | Destination path for processed files |
| `sftp.include.metadata` | Boolean | `false` | Include metadata in JSON output |
| `sftp.max.file.size.bytes` | Long | `104857600` | Maximum file size (100MB default) |
| `sftp.min.file.size.bytes` | Long | `0` | Minimum file size |
| `sftp.start.from` | String | `latest` | Where to start: `latest` or `earliest` |
| `sftp.host.key.fingerprint` | String | | Host key fingerprint for verification |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `topics` | String | Comma-separated list of topics to consume |
| `sftp.host` | String | SFTP server hostname |
| `sftp.username` | String | SSH username |

### Authentication (one required)

| Setting | Type | Description |
|---------|------|-------------|
| `sftp.password` | Password | SSH password |
| `sftp.private.key.path` | String | Path to SSH private key file |
| `sftp.private.key.content` | String | SSH private key as string |
| `sftp.private.key.passphrase` | Password | Passphrase for encrypted private key |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `sftp.port` | Int | `22` | SFTP server port |
| `sftp.timeout.seconds` | Int | `30` | Connection and operation timeout |
| `sftp.output.path` | String | `/` | Output path template |
| `sftp.output.mode` | String | `file` | Output mode: `file` or `append` |
| `sftp.file.name.field` | String | | JSON field containing the filename |
| `sftp.content.field` | String | | JSON field containing the file content |
| `sftp.create.directories` | Boolean | `true` | Create directories if they don't exist |
| `sftp.overwrite` | Boolean | `true` | Overwrite existing files |
| `sftp.temp.suffix` | String | `.tmp` | Suffix for temporary files during upload |
| `sftp.flush.interval.ms` | Int | `10000` | Flush interval in milliseconds |

## Examples

### Watch Directory for New Files

```json
{
  "name": "sftp-file-watcher",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSourceConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "user",
  "sftp.password": "${secrets:sftp-password}",
  "sftp.remote.path": "/incoming",
  "sftp.file.pattern": "*.csv",
  "sftp.poll.interval.ms": 10000,
  "topic": "incoming-files"
}
```

### Watch with SSH Key Authentication

```json
{
  "name": "sftp-ssh-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSourceConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "automation",
  "sftp.private.key.path": "/home/user/.ssh/id_rsa",
  "sftp.private.key.passphrase": "${secrets:ssh-passphrase}",
  "sftp.remote.path": "/data",
  "sftp.recursive": true,
  "topic": "sftp-data"
}
```

### Move Files After Processing

```json
{
  "name": "sftp-with-move",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSourceConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "user",
  "sftp.password": "${secrets:sftp-password}",
  "sftp.remote.path": "/incoming",
  "sftp.move.after.read": true,
  "sftp.move.to.path": "/processed",
  "topic": "processed-files"
}
```

### Delete Files After Processing

```json
{
  "name": "sftp-with-delete",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSourceConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "user",
  "sftp.password": "${secrets:sftp-password}",
  "sftp.remote.path": "/incoming",
  "sftp.delete.after.read": true,
  "topic": "deleted-after-read"
}
```

### Include Metadata in Output

```json
{
  "name": "sftp-with-metadata",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSourceConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "user",
  "sftp.password": "${secrets:sftp-password}",
  "sftp.remote.path": "/data",
  "sftp.include.metadata": true,
  "topic": "files-with-metadata"
}
```

### Upload Files to SFTP Server

```json
{
  "name": "sftp-file-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSinkConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "user",
  "sftp.password": "${secrets:sftp-password}",
  "sftp.output.path": "/outgoing/${topic}",
  "sftp.file.name.field": "filename",
  "sftp.content.field": "data",
  "topics": "outgoing-files"
}
```

### Append to Log File

```json
{
  "name": "sftp-log-appender",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSinkConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "user",
  "sftp.password": "${secrets:sftp-password}",
  "sftp.output.path": "/logs/${topic}.log",
  "sftp.output.mode": "append",
  "topics": "application-logs"
}
```

### Template-Based Output Paths

```json
{
  "name": "sftp-templated-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSinkConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "user",
  "sftp.password": "${secrets:sftp-password}",
  "sftp.output.path": "/data/${topic}/${key}/${timestamp}.json",
  "sftp.create.directories": true,
  "topics": "partitioned-data"
}
```

### Non-Overwriting Sink

```json
{
  "name": "sftp-no-overwrite",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Sftp.SftpSinkConnector",
  "sftp.host": "sftp.example.com",
  "sftp.username": "user",
  "sftp.password": "${secrets:sftp-password}",
  "sftp.output.path": "/archive/${timestamp}_${offset}.dat",
  "sftp.overwrite": false,
  "topics": "archive-data"
}
```

## Source Record Formats

### Raw Content Mode (default)

When `sftp.include.metadata` is `false`, records contain the raw file content as bytes.

### Metadata Mode

When `sftp.include.metadata` is `true`, records are JSON:

```json
{
  "path": "/incoming/data.csv",
  "name": "data.csv",
  "size": 1024,
  "lastModified": "2024-01-15T10:30:00+00:00",
  "content": "base64-encoded-content..."
}
```

## Sink Input Format

The sink accepts records in multiple formats:

### JSON with Field Mapping

```json
{
  "filename": "output.json",
  "data": "base64-encoded-content-or-raw-string"
}
```

Configure `sftp.file.name.field` and `sftp.content.field` to specify the fields.

### Raw Content

When no field mapping is configured, the entire record value is written to a file.
The filename is generated from the output path template.

## Path Template Variables

The sink supports these variables in `sftp.output.path`:

| Variable | Description |
|----------|-------------|
| `${topic}` | Source topic name |
| `${partition}` | Topic partition number |
| `${offset}` | Record offset |
| `${key}` | Record key (sanitized for filenames) |
| `${timestamp}` | Current timestamp (yyyyMMddHHmmss) |

## File Pattern Syntax

The source connector supports glob-style patterns:

| Pattern | Description |
|---------|-------------|
| `*` | Match any characters |
| `?` | Match single character |
| `*.csv` | All CSV files |
| `data_*.json` | JSON files starting with "data_" |
| `report_???.xlsx` | XLSX files with 3-char suffix |

## Authentication

### Password Authentication

```json
{
  "sftp.username": "user",
  "sftp.password": "secret"
}
```

### SSH Key from File

```json
{
  "sftp.username": "user",
  "sftp.private.key.path": "/path/to/id_rsa",
  "sftp.private.key.passphrase": "optional-passphrase"
}
```

### SSH Key from Content

```json
{
  "sftp.username": "user",
  "sftp.private.key.content": "-----BEGIN OPENSSH PRIVATE KEY-----\n...",
  "sftp.private.key.passphrase": "optional-passphrase"
}
```

## Performance Considerations

- **Poll Interval**: Balance between freshness and server load (10-60 seconds typical)
- **File Size Limits**: Set appropriate `max.file.size.bytes` to prevent memory issues
- **Recursive Scanning**: Can be slow for deep directory structures
- **Temporary Files**: Use `sftp.temp.suffix` for atomic writes
- **Connection Reuse**: Connections are kept open between operations

## Error Handling

- Connection failures trigger automatic reconnection on next operation
- File upload uses temporary suffix for atomic writes
- Invalid JSON records are skipped in sink operations
- Files exceeding size limits are skipped

## Limitations

- No support for FTPS (FTP over SSL) - use SFTP only
- No support for multi-part uploads
- No support for file locking
- Host key verification is optional (use `sftp.host.key.fingerprint` for security)
- Symlinks are treated as regular files
- No support for SCP protocol

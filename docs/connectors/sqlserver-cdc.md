# SQL Server CDC Source Connector

The SQL Server CDC (Change Data Capture) connector captures row-level changes from Microsoft SQL Server databases using the built-in CDC feature. It polls CDC change tables for INSERT, UPDATE, and DELETE events and produces Debezium-compatible JSON output.

## Features

- **Change Data Capture**: Uses SQL Server's native CDC feature with change tables
- **Initial Snapshot**: Optional snapshot of existing data before streaming changes
- **LSN Tracking**: Position tracking via Log Sequence Numbers (LSN)
- **Debezium-Compatible Output**: Standard CDC format with op, before, after, source fields
- **Windows/SQL Authentication**: Supports both Integrated Security and SQL authentication
- **Multiple Tables**: Capture changes from multiple tables simultaneously
- **Flexible Topic Naming**: Configurable topic patterns with schema/table variables

## Installation

The connector is included in the `Kuestenlogik.Surgewave.Connect.SqlServer` package.

## SQL Server Requirements

### Enable CDC on Database

```sql
-- Enable CDC on the database
USE YourDatabase;
GO
EXEC sys.sp_cdc_enable_db;
GO
```

### Enable CDC on Tables

```sql
-- Enable CDC on a table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'YourTable',
    @role_name = NULL,
    @supports_net_changes = 1;
GO
```

### Required Permissions

The user needs:
- `db_owner` role or explicit permissions to query CDC tables
- `SELECT` on `cdc.change_tables` and `cdc.captured_columns`
- `EXECUTE` on `sys.fn_cdc_get_min_lsn` and `sys.fn_cdc_get_max_lsn`
- `SELECT` on `cdc.fn_cdc_get_all_changes_<capture_instance>`

## Configuration

### Required Settings

| Property | Description |
|----------|-------------|
| `sqlserver.database` | Database name (or use connection string) |
| `sqlserver.tables` | Comma-separated list of tables to capture |

### Connection Settings

| Property | Default | Description |
|----------|---------|-------------|
| `sqlserver.connection.string` | (empty) | Full connection string (alternative to individual settings) |
| `sqlserver.server` | `localhost` | SQL Server hostname |
| `sqlserver.username` | (empty) | Username (empty for Windows auth) |
| `sqlserver.password` | (empty) | Password |
| `sqlserver.trust.server.certificate` | `false` | Trust server certificate |
| `sqlserver.encrypt` | `true` | Encrypt connection |

### CDC Settings

| Property | Default | Description |
|----------|---------|-------------|
| `sqlserver.topic.prefix` | (empty) | Prefix for topic names |
| `sqlserver.topic.pattern` | `${schema}.${table}` | Topic naming pattern |
| `sqlserver.include.schema` | `true` | Include schema in topic name |
| `sqlserver.include.before.values` | `true` | Include before values in updates |
| `sqlserver.snapshot.mode` | `initial` | Snapshot mode (see below) |
| `sqlserver.poll.interval.ms` | `500` | Poll interval in milliseconds |
| `sqlserver.batch.max.records` | `1000` | Maximum records per batch |
| `sqlserver.start.from.beginning` | `false` | Start from beginning of CDC history |

### Snapshot Modes

| Mode | Description |
|------|-------------|
| `initial` | Perform initial snapshot, then stream changes |
| `never` | Skip snapshot, only stream changes |
| `always` | Always perform snapshot on startup |
| `schema_only` | Snapshot schema only, then stream changes |

## Output Format

Records are produced in Debezium-compatible JSON format:

```json
{
  "op": "c",
  "source": {
    "schema": "dbo",
    "table": "users",
    "server": "localhost",
    "lsn": "00000025000000000001"
  },
  "before": null,
  "after": {
    "Id": 1,
    "Name": "John",
    "Email": "john@example.com"
  },
  "ts_ms": 1704067200000
}
```

### Operation Types

| Op | Description |
|----|-------------|
| `c` | Create (INSERT) |
| `u` | Update |
| `d` | Delete |
| `r` | Read (snapshot) |

## Headers

Each record includes the following headers:

| Header | Description |
|--------|-------------|
| `sqlserver.schema` | Source schema name |
| `sqlserver.table` | Source table name |
| `sqlserver.op` | Operation type |

## Example Usage

### Basic Configuration with Windows Auth

```csharp
var connector = new SqlServerCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["sqlserver.server"] = "localhost",
    ["sqlserver.database"] = "ecommerce",
    ["sqlserver.tables"] = "dbo.users,dbo.orders,sales.products"
});
```

### With SQL Authentication

```csharp
var connector = new SqlServerCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["sqlserver.server"] = "sqlserver.example.com",
    ["sqlserver.database"] = "production",
    ["sqlserver.username"] = "cdc_user",
    ["sqlserver.password"] = "SecurePassword123!",
    ["sqlserver.tables"] = "dbo.customers,dbo.transactions",
    ["sqlserver.trust.server.certificate"] = "true",
    ["sqlserver.topic.prefix"] = "cdc."
});
```

### Using Connection String

```csharp
var connector = new SqlServerCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["sqlserver.connection.string"] = "Server=myserver;Database=mydb;User Id=sa;Password=secret;TrustServerCertificate=true",
    ["sqlserver.tables"] = "dbo.events",
    ["sqlserver.snapshot.mode"] = "never"
});
```

### Starting from Beginning of CDC History

```csharp
var connector = new SqlServerCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["sqlserver.database"] = "realtime_db",
    ["sqlserver.tables"] = "dbo.audit_log",
    ["sqlserver.snapshot.mode"] = "never",
    ["sqlserver.start.from.beginning"] = "true"
});
```

## Offset Management

The connector tracks its position using:

- `lsn`: Current Log Sequence Number (hex-encoded)
- `snapshot.completed`: Whether initial snapshot is complete

Offsets are stored via the Connect framework's offset storage and restored on restart.

## CDC Operation Values

SQL Server CDC uses the following operation codes in the `__$operation` column:

| Value | Description |
|-------|-------------|
| 1 | Delete |
| 2 | Insert |
| 3 | Update (before image) |
| 4 | Update (after image) |

The connector pairs operations 3 and 4 to produce a single update record with both before and after values (when `include.before.values` is true).

## Limitations

- Requires SQL Server 2008 or later with CDC enabled
- CDC must be enabled on each table individually
- Single task per connector (CDC polling is sequential)
- CDC retention period limits how far back changes can be read
- Schema changes require re-enabling CDC on affected tables

## Troubleshooting

### CDC Not Enabled Error

If you see "CDC is not enabled for table X", ensure:
1. CDC is enabled on the database: `EXEC sys.sp_cdc_enable_db`
2. CDC is enabled on the table: `EXEC sys.sp_cdc_enable_table`

### Connection Issues

1. Verify SQL Server allows remote connections
2. Check firewall rules for port 1433
3. For TLS issues, try `TrustServerCertificate=true`
4. For Windows auth, ensure the application runs under appropriate credentials

### Missing Changes

1. Check CDC retention period: `SELECT * FROM msdb.dbo.cdc_jobs`
2. Verify the SQL Server Agent is running (required for CDC cleanup)
3. Ensure the connector's LSN is within the CDC retention window

### Performance Tuning

- Increase `poll.interval.ms` to reduce database load
- Adjust `batch.max.records` for throughput vs latency trade-off
- Consider partitioning large tables for better CDC performance

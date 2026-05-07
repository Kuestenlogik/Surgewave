# MySQL/MariaDB CDC Source Connector

The MySQL CDC (Change Data Capture) connector captures row-level changes from MySQL and MariaDB databases using binary log (binlog) replication. It produces Debezium-compatible JSON output with operation types, before/after values, and source metadata.

## Features

- **Binary Log Replication**: Captures INSERT, UPDATE, and DELETE events in real-time
- **Initial Snapshot**: Optional snapshot of existing data before streaming changes
- **GTID Support**: Position tracking via Global Transaction IDs or file/position
- **Debezium-Compatible Output**: Standard CDC format with op, before, after, source fields
- **SSL/TLS**: Secure connections with configurable SSL modes
- **Multiple Tables**: Capture changes from multiple tables simultaneously
- **Flexible Topic Naming**: Configurable topic patterns with database/table variables

## Installation

The connector is included in the `Kuestenlogik.Surgewave.Connect.MySql` package.

## Configuration

### Required Settings

| Property | Description |
|----------|-------------|
| `mysql.database` | Database name to connect to |
| `mysql.username` | MySQL username for replication |
| `mysql.tables` | Comma-separated list of tables to capture |

### Optional Settings

| Property | Default | Description |
|----------|---------|-------------|
| `mysql.host` | `localhost` | MySQL server hostname |
| `mysql.port` | `3306` | MySQL server port |
| `mysql.password` | (empty) | MySQL password |
| `mysql.server.id` | `65535` | Unique server ID for binlog replication |
| `mysql.topic.prefix` | (empty) | Prefix for topic names |
| `mysql.topic.pattern` | `${database}.${table}` | Topic naming pattern |
| `mysql.include.schema` | `true` | Include database name in topic |
| `mysql.include.before.values` | `true` | Include before values in updates |
| `mysql.snapshot.mode` | `initial` | Snapshot mode (see below) |
| `mysql.ssl.mode` | `none` | SSL mode (see below) |
| `mysql.poll.interval.ms` | `100` | Poll interval in milliseconds |
| `mysql.batch.max.records` | `1000` | Maximum records per batch |

### Snapshot Modes

| Mode | Description |
|------|-------------|
| `initial` | Perform initial snapshot, then stream changes |
| `never` | Skip snapshot, only stream changes |
| `always` | Always perform snapshot on startup |
| `schema_only` | Snapshot schema only, then stream changes |

### SSL Modes

| Mode | Description |
|------|-------------|
| `none` | No SSL encryption |
| `preferred` | Use SSL if available |
| `required` | Require SSL connection |
| `verify_ca` | Require SSL and verify CA |
| `verify_full` | Require SSL and verify CA + hostname |

### Binlog Position (Optional)

| Property | Description |
|----------|-------------|
| `mysql.binlog.filename` | Start from specific binlog file |
| `mysql.binlog.position` | Start from specific position |
| `mysql.gtid.set` | Start from specific GTID set |

## Output Format

Records are produced in Debezium-compatible JSON format:

```json
{
  "op": "c",
  "source": {
    "database": "mydb",
    "table": "users",
    "server": "localhost:3306",
    "binlog_file": "mysql-bin.000001",
    "binlog_position": 12345
  },
  "before": null,
  "after": {
    "col_0": 1,
    "col_1": "John",
    "col_2": "john@example.com"
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
| `mysql.database` | Source database name |
| `mysql.table` | Source table name |
| `mysql.op` | Operation type |

## MySQL Server Requirements

### Binary Logging

Enable binary logging in MySQL configuration:

```ini
[mysqld]
server-id=1
log_bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
```

### Required Privileges

The user needs the following privileges:

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'cdc_user'@'%';
```

## Example Usage

### Basic Configuration

```csharp
var connector = new MySqlCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["mysql.host"] = "localhost",
    ["mysql.database"] = "ecommerce",
    ["mysql.username"] = "cdc_user",
    ["mysql.password"] = "secret",
    ["mysql.tables"] = "users,orders,products"
});
```

### With SSL and Custom Topic Pattern

```csharp
var connector = new MySqlCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["mysql.host"] = "mysql.example.com",
    ["mysql.database"] = "production",
    ["mysql.username"] = "replicator",
    ["mysql.password"] = "secure_password",
    ["mysql.tables"] = "customers,transactions",
    ["mysql.ssl.mode"] = "required",
    ["mysql.topic.prefix"] = "cdc.",
    ["mysql.topic.pattern"] = "${database}.${table}",
    ["mysql.snapshot.mode"] = "initial"
});
```

### Streaming Without Snapshot

```csharp
var connector = new MySqlCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["mysql.database"] = "realtime_db",
    ["mysql.username"] = "stream_user",
    ["mysql.tables"] = "events",
    ["mysql.snapshot.mode"] = "never",
    ["mysql.binlog.filename"] = "mysql-bin.000010",
    ["mysql.binlog.position"] = "4"
});
```

## Offset Management

The connector tracks its position using:

- `binlog.filename`: Current binlog file
- `binlog.position`: Position within the binlog file
- `gtid`: GTID if available
- `snapshot.completed`: Whether initial snapshot is complete

Offsets are stored via the Connect framework's offset storage and restored on restart.

## Limitations

- Column names are not available in binlog events for all MySQL versions; uses positional column names (`col_0`, `col_1`, etc.)
- Only supports ROW format binary logging (not STATEMENT or MIXED)
- Single task per connector (binlog replication is sequential)

## Troubleshooting

### Connection Issues

1. Verify MySQL allows remote connections
2. Check firewall rules for port 3306
3. Ensure the user has REPLICATION SLAVE privilege

### Missing Events

1. Verify `binlog_format` is set to ROW
2. Check `binlog_row_image` is FULL for UPDATE events
3. Ensure the connector's server-id is unique

### SSL Errors

1. Verify SSL certificates are valid
2. Check MySQL server supports the requested SSL mode
3. For `verify_full`, ensure hostname matches certificate CN

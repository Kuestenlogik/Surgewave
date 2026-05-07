# Oracle CDC Source Connector

The Oracle CDC (Change Data Capture) connector captures row-level changes from Oracle databases using LogMiner. It reads redo logs for INSERT, UPDATE, and DELETE events and produces Debezium-compatible JSON output.

## Features

- **LogMiner Integration**: Uses Oracle's built-in LogMiner for reading redo logs
- **Initial Snapshot**: Optional snapshot of existing data before streaming changes
- **SCN Tracking**: Position tracking via System Change Numbers (SCN)
- **Debezium-Compatible Output**: Standard CDC format with op, before, after, source fields
- **Service Name/SID Support**: Connect using service name or SID
- **Multiple Tables**: Capture changes from multiple tables simultaneously
- **Flexible Topic Naming**: Configurable topic patterns with owner/table variables

## Installation

The connector is included in the `Kuestenlogik.Surgewave.Connect.Oracle` package.

## Oracle Requirements

### Enable Supplemental Logging

```sql
-- Enable database-level supplemental logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Enable minimal supplemental logging (required)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- For specific tables, enable table-level supplemental logging
ALTER TABLE hr.employees ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### LogMiner Permissions

The user needs the following privileges:

```sql
-- Grant LogMiner privileges
GRANT EXECUTE ON DBMS_LOGMNR TO cdc_user;
GRANT EXECUTE ON DBMS_LOGMNR_D TO cdc_user;
GRANT SELECT ON V$LOGMNR_CONTENTS TO cdc_user;
GRANT SELECT ON V$LOGMNR_LOGS TO cdc_user;
GRANT SELECT ON V$LOGFILE TO cdc_user;
GRANT SELECT ON V$ARCHIVED_LOG TO cdc_user;
GRANT SELECT ON V$DATABASE TO cdc_user;
GRANT SELECT ON V$LOG TO cdc_user;

-- Grant access to tables being captured
GRANT SELECT ON hr.employees TO cdc_user;
GRANT SELECT ON hr.departments TO cdc_user;

-- Grant access to data dictionary
GRANT SELECT ON ALL_TAB_COLUMNS TO cdc_user;
```

### Archive Log Mode

For production use, archive log mode should be enabled:

```sql
-- Check current mode
SELECT log_mode FROM v$database;

-- Enable archive log mode (requires restart)
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
```

## Configuration

### Required Settings

| Property | Description |
|----------|-------------|
| `oracle.service.name` or `oracle.sid` | Oracle service name or SID (or use connection string) |
| `oracle.tables` | Comma-separated list of tables to capture |

### Connection Settings

| Property | Default | Description |
|----------|---------|-------------|
| `oracle.connection.string` | (empty) | Full connection string (alternative to individual settings) |
| `oracle.host` | `localhost` | Oracle hostname |
| `oracle.port` | `1521` | Oracle port |
| `oracle.service.name` | (empty) | Oracle service name |
| `oracle.sid` | (empty) | Oracle SID (alternative to service name) |
| `oracle.username` | (empty) | Username |
| `oracle.password` | (empty) | Password |
| `oracle.wallet.location` | (empty) | Oracle wallet location for secure password store |

### CDC Settings

| Property | Default | Description |
|----------|---------|-------------|
| `oracle.topic.prefix` | (empty) | Prefix for topic names |
| `oracle.topic.pattern` | `${owner}.${table}` | Topic naming pattern |
| `oracle.include.schema` | `true` | Include owner in topic name |
| `oracle.include.before.values` | `true` | Include before values in updates |
| `oracle.snapshot.mode` | `initial` | Snapshot mode (see below) |
| `oracle.poll.interval.ms` | `500` | Poll interval in milliseconds |
| `oracle.batch.max.records` | `1000` | Maximum records per batch |
| `oracle.start.from.beginning` | `false` | Start from beginning of available logs |

### LogMiner Settings

| Property | Default | Description |
|----------|---------|-------------|
| `oracle.logminer.mode` | `online` | LogMiner mode (online, archived) |
| `oracle.dictionary.mode` | `online` | Dictionary mode (online, redo_log) |

### Snapshot Modes

| Mode | Description |
|------|-------------|
| `initial` | Perform initial snapshot, then stream changes |
| `never` | Skip snapshot, only stream changes |
| `always` | Always perform snapshot on startup |
| `schema_only` | Snapshot schema only, then stream changes |

### LogMiner Modes

| Mode | Description |
|------|-------------|
| `online` | Use online redo logs (default) |
| `archived` | Use archived redo logs only |

### Dictionary Modes

| Mode | Description |
|------|-------------|
| `online` | Use online catalog for dictionary (faster) |
| `redo_log` | Extract dictionary from redo logs (more accurate for DDL) |

## Output Format

Records are produced in Debezium-compatible JSON format:

```json
{
  "op": "c",
  "source": {
    "owner": "HR",
    "table": "EMPLOYEES",
    "host": "localhost",
    "scn": 1234567890,
    "timestamp": "2024-01-15T10:30:00.000Z"
  },
  "before": null,
  "after": {
    "EMPLOYEE_ID": 1,
    "FIRST_NAME": "John",
    "LAST_NAME": "Doe",
    "EMAIL": "john.doe@example.com"
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
| `oracle.owner` | Source owner/schema name |
| `oracle.table` | Source table name |
| `oracle.op` | Operation type |
| `oracle.scn` | System Change Number |

## Example Usage

### Basic Configuration with Service Name

```csharp
var connector = new OracleCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["oracle.host"] = "localhost",
    ["oracle.service.name"] = "ORCL",
    ["oracle.username"] = "cdc_user",
    ["oracle.password"] = "secret",
    ["oracle.tables"] = "HR.EMPLOYEES,HR.DEPARTMENTS,SALES.ORDERS"
});
```

### With SID

```csharp
var connector = new OracleCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["oracle.host"] = "oracle.example.com",
    ["oracle.port"] = "1521",
    ["oracle.sid"] = "XE",
    ["oracle.username"] = "system",
    ["oracle.password"] = "oracle",
    ["oracle.tables"] = "USERS,AUDIT_LOG",
    ["oracle.topic.prefix"] = "cdc."
});
```

### Using Connection String

```csharp
var connector = new OracleCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["oracle.connection.string"] = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)));User Id=cdc_user;Password=secret",
    ["oracle.tables"] = "HR.EMPLOYEES",
    ["oracle.snapshot.mode"] = "never"
});
```

### Starting from Beginning of Logs

```csharp
var connector = new OracleCdcSourceConnector();
connector.Start(new Dictionary<string, string>
{
    ["oracle.service.name"] = "ORCL",
    ["oracle.username"] = "cdc_user",
    ["oracle.password"] = "secret",
    ["oracle.tables"] = "AUDIT.EVENTS",
    ["oracle.snapshot.mode"] = "never",
    ["oracle.start.from.beginning"] = "true"
});
```

## Offset Management

The connector tracks its position using:

- `scn`: Current System Change Number
- `snapshot.completed`: Whether initial snapshot is complete

Offsets are stored via the Connect framework's offset storage and restored on restart.

## LogMiner Operation Codes

Oracle LogMiner uses the following operation codes in V$LOGMNR_CONTENTS:

| Code | Description |
|------|-------------|
| 1 | INSERT |
| 2 | DELETE |
| 3 | UPDATE |
| 5 | DDL |
| 6 | START (transaction) |
| 7 | COMMIT |
| 36 | ROLLBACK |

The connector captures INSERT (1), DELETE (2), and UPDATE (3) operations.

## Limitations

- Requires Oracle Database 10g or later with LogMiner support
- Supplemental logging must be enabled on database and tables
- Single task per connector (LogMiner polling is sequential)
- Redo log retention limits how far back changes can be read
- Schema changes may require restarting the connector
- Large transactions may cause memory pressure
- DDL changes are not captured (only DML)

## Troubleshooting

### LogMiner Permissions Error

If you see "ORA-01031: insufficient privileges":
1. Verify all required grants are in place
2. Check that user has SELECT on V$ views
3. Ensure EXECUTE on DBMS_LOGMNR is granted

### Missing Redo Logs

If you see "ORA-01291: missing log file":
1. Check archive log retention settings
2. Verify archived logs are not being deleted too quickly
3. Consider increasing log retention period

### No Changes Detected

1. Verify supplemental logging is enabled on the table
2. Check that changes are being committed (not just in uncommitted transactions)
3. Ensure the SCN is within the available log range

### Performance Tuning

- Increase `poll.interval.ms` to reduce database load
- Adjust `batch.max.records` for throughput vs latency trade-off
- Use `archived` log mode for lower impact on active database
- Enable table-level supplemental logging only on needed tables
- Consider partitioning for large tables

### Connection Issues

1. Verify Oracle listener is running
2. Check firewall rules for port 1521
3. For TNS issues, verify tnsnames.ora configuration
4. Test connection with SQL*Plus or SQL Developer first

## Best Practices

1. **Use a dedicated CDC user** with minimal required privileges
2. **Enable supplemental logging** only on tables you need to capture
3. **Monitor redo log space** - LogMiner increases redo generation
4. **Set appropriate log retention** to allow for connector restarts
5. **Use service names** instead of SIDs for RAC environments
6. **Test in non-production first** before deploying to production

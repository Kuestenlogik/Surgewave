# Generic Database Connector

The Generic Database connector works with any ADO.NET-compatible database, including SQL Server, PostgreSQL, MySQL, SQLite, Oracle, and more.

## Overview

- **Source**: Query databases using bulk, incrementing, or timestamp modes
- **Sink**: Insert, upsert, or update records in database tables

**Use Cases:**
- Database-to-database synchronization
- Batch data extraction
- Event logging to SQL databases
- Legacy system integration

## Quick Start

### Database Source

Query data from a database:

```json
{
  "name": "db-source",
  "config": {
    "connector.class": "DatabaseSourceConnector",
    "database.provider": "Npgsql",
    "database.connection.string": "Host=localhost;Database=mydb;Username=user;Password=pass",
    "database.table": "events",
    "topic": "db-events",
    "mode": "incrementing",
    "incrementing.column": "id"
  }
}
```

### Database Sink

Write data to a database:

```json
{
  "name": "db-sink",
  "config": {
    "connector.class": "DatabaseSinkConnector",
    "database.provider": "Npgsql",
    "database.connection.string": "Host=localhost;Database=mydb;Username=user;Password=pass",
    "topics": "user-events",
    "database.table": "event_log",
    "write.mode": "insert"
  }
}
```

## Configuration Reference

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `database.provider` | string | Required | ADO.NET provider name |
| `database.connection.string` | string | Required | Connection string |
| `database.table` | string | - | Table name (supports `${topic}` pattern) |

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topic` | string | Required | Destination Surgewave topic |
| `mode` | string | `bulk` | Mode: `bulk`, `incrementing`, `timestamp`, `timestamp+incrementing` |
| `incrementing.column` | string | - | Auto-incrementing column |
| `timestamp.column` | string | - | Timestamp column |
| `query` | string | - | Custom SQL query |
| `poll.interval.ms` | long | `10000` | Polling interval |
| `batch.size` | int | `1000` | Rows per batch |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `write.mode` | string | `insert` | Mode: `insert`, `upsert`, `update` |
| `pk.fields` | string | - | Primary key fields for upsert/update |
| `batch.size` | int | `1000` | Records per batch |

## Supported Providers

### Provider Names

| Database | Provider Name | NuGet Package |
|----------|---------------|---------------|
| PostgreSQL | `Npgsql` | Npgsql |
| SQL Server | `Microsoft.Data.SqlClient` | Microsoft.Data.SqlClient |
| MySQL | `MySql.Data.MySqlClient` | MySql.Data |
| SQLite | `Microsoft.Data.Sqlite` | Microsoft.Data.Sqlite |
| Oracle | `Oracle.ManagedDataAccess.Client` | Oracle.ManagedDataAccess.Core |

### Connection String Examples

**PostgreSQL:**
```
Host=localhost;Port=5432;Database=mydb;Username=user;Password=pass
```

**SQL Server:**
```
Server=localhost;Database=mydb;User Id=user;Password=pass;TrustServerCertificate=True
```

**MySQL:**
```
Server=localhost;Port=3306;Database=mydb;Uid=user;Pwd=pass
```

**SQLite:**
```
Data Source=/path/to/database.db
```

## Source Modes

### Bulk Mode

Reads entire table on each poll (for small tables or full syncs):

```json
{
  "mode": "bulk",
  "poll.interval.ms": "60000"
}
```

### Incrementing Mode

Tracks progress using an auto-incrementing column:

```json
{
  "mode": "incrementing",
  "incrementing.column": "id"
}
```

Only reads rows where `id > last_seen_id`.

### Timestamp Mode

Tracks progress using a timestamp column:

```json
{
  "mode": "timestamp",
  "timestamp.column": "updated_at"
}
```

Only reads rows where `updated_at > last_seen_timestamp`.

### Timestamp + Incrementing

Combines both for reliable change detection:

```json
{
  "mode": "timestamp+incrementing",
  "timestamp.column": "updated_at",
  "incrementing.column": "id"
}
```

## Custom Queries

Use custom SQL for complex extractions:

```json
{
  "query": "SELECT id, name, email, updated_at FROM users WHERE status = 'active' AND updated_at > :timestamp ORDER BY updated_at, id",
  "mode": "timestamp+incrementing",
  "timestamp.column": "updated_at",
  "incrementing.column": "id"
}
```

Parameters:
- `:timestamp` - Last seen timestamp
- `:incrementing` - Last seen incrementing value

## Sink Write Modes

### Insert

Insert new rows (fails on duplicates):

```json
{
  "write.mode": "insert"
}
```

### Upsert

Insert or update based on primary key:

```json
{
  "write.mode": "upsert",
  "pk.fields": "id"
}
```

Generates database-specific upsert syntax:
- PostgreSQL: `INSERT ... ON CONFLICT DO UPDATE`
- SQL Server: `MERGE`
- MySQL: `INSERT ... ON DUPLICATE KEY UPDATE`

### Update

Update existing rows only:

```json
{
  "write.mode": "update",
  "pk.fields": "id"
}
```

## Table Name Patterns

Use `${topic}` to dynamically set table name:

```json
{
  "database.table": "${topic}_events"
}
```

Topic `user-actions` writes to table `user-actions_events`.

## Examples

### SQL Server to PostgreSQL

Replicate data between databases:

**Source (SQL Server):**
```json
{
  "name": "sqlserver-source",
  "config": {
    "connector.class": "DatabaseSourceConnector",
    "database.provider": "Microsoft.Data.SqlClient",
    "database.connection.string": "Server=sql.example.com;Database=source;User Id=reader;Password=pass",
    "database.table": "orders",
    "topic": "orders-sync",
    "mode": "timestamp+incrementing",
    "timestamp.column": "modified_at",
    "incrementing.column": "order_id"
  }
}
```

**Sink (PostgreSQL):**
```json
{
  "name": "postgres-sink",
  "config": {
    "connector.class": "DatabaseSinkConnector",
    "database.provider": "Npgsql",
    "database.connection.string": "Host=pg.example.com;Database=dest;Username=writer;Password=pass",
    "topics": "orders-sync",
    "database.table": "orders",
    "write.mode": "upsert",
    "pk.fields": "order_id"
  }
}
```

### Event Logging

Log Surgewave events to a database:

```json
{
  "name": "event-logger",
  "config": {
    "connector.class": "DatabaseSinkConnector",
    "database.provider": "Npgsql",
    "database.connection.string": "Host=localhost;Database=logs;Username=logger;Password=pass",
    "topics": "application-events,system-events",
    "database.table": "${topic}",
    "write.mode": "insert",
    "batch.size": "500"
  }
}
```

### SQLite for Testing

Local development with SQLite:

```json
{
  "name": "sqlite-sink",
  "config": {
    "connector.class": "DatabaseSinkConnector",
    "database.provider": "Microsoft.Data.Sqlite",
    "database.connection.string": "Data Source=test.db",
    "topics": "test-events",
    "database.table": "events",
    "write.mode": "insert"
  }
}
```

## Schema Requirements

### Source Tables

Tables should have:
- Primary key or unique incrementing column
- Timestamp column for change tracking (recommended)
- Index on tracking columns for performance

### Sink Tables

Create tables before starting sink connector:

```sql
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(255),
    key BYTEA,
    value JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Troubleshooting

### Common Issues

**Connection Refused**
- Verify database server is running
- Check firewall rules
- Ensure connection string is correct

**Permission Denied**
- Verify user has SELECT permission (source)
- Verify user has INSERT/UPDATE permission (sink)
- Check table ownership

**Slow Performance**
- Add indexes on tracking columns
- Increase `batch.size`
- Reduce `poll.interval.ms`

### Provider Registration

If provider not found, register it in code:

```csharp
DbProviderFactories.RegisterFactory("Npgsql", Npgsql.NpgsqlFactory.Instance);
DbProviderFactories.RegisterFactory("MySql.Data.MySqlClient", MySql.Data.MySqlClient.MySqlClientFactory.Instance);
```

## See Also

- [PostgreSQL CDC Connector](postgresql.md)
- [MongoDB Connector](mongodb.md)
- [Custom Connectors](custom-connectors.md)

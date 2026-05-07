# Snowflake Connector

The Snowflake connector provides integration with Snowflake Data Cloud for both reading from and writing to Snowflake tables.

## Package

```
Kuestenlogik.Surgewave.Connect.Snowflake
```

## Features

### Source Connector
- **Table Mode**: Poll data from tables with optional incremental loading
- **Query Mode**: Execute custom SQL queries for data extraction
- **Stream Mode**: Use Snowflake Streams for real-time CDC (Change Data Capture)
- Incremental polling with timestamp or incrementing columns
- Automatic offset tracking for exactly-once delivery
- Rich metadata in record headers

### Sink Connector
- **Insert Mode**: Standard INSERT operations for new records
- **Upsert/Merge Mode**: MERGE operations for insert-or-update semantics
- Batch processing for high throughput
- Auto-create tables based on record schema
- Retry with exponential backoff for transient failures
- Tombstone handling for deletes

## Authentication Methods

- **Password**: Standard username/password authentication
- **Key-Pair**: RSA key-pair authentication with private key file
- **OAuth**: OAuth 2.0 access token
- **External Browser**: Interactive browser-based SSO

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `snowflake.account` | String | Snowflake account identifier (e.g., `abc12345.us-east-1`) |
| `snowflake.user` | String | Snowflake username |
| `snowflake.database` | String | Database name |
| `snowflake.table` | String | Table name (required for table/stream mode) |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `snowflake.password` | Password | | Snowflake password |
| `snowflake.schema` | String | `PUBLIC` | Schema name |
| `snowflake.warehouse` | String | | Warehouse name |
| `snowflake.role` | String | | Role name |
| `snowflake.authenticator` | String | `snowflake` | Auth method: `snowflake`, `externalbrowser`, `oauth`, `jwt` |
| `snowflake.private.key.file` | String | | Path to private key file (for key-pair auth) |
| `snowflake.private.key.passphrase` | Password | | Private key passphrase |
| `snowflake.oauth.token` | Password | | OAuth access token |
| `snowflake.mode` | String | `table` | Mode: `table`, `query`, `stream` |
| `snowflake.query` | String | | Custom SQL query (for query mode) |
| `snowflake.stream.name` | String | | Stream name (auto-created if not exists) |
| `snowflake.topic.pattern` | String | `snowflake.${database}.${schema}.${table}` | Topic naming pattern |
| `snowflake.poll.interval.ms` | Int | `5000` | Poll interval in milliseconds |
| `snowflake.max.rows.per.poll` | Int | `10000` | Maximum rows per poll |
| `snowflake.include.metadata` | Boolean | `true` | Include Snowflake metadata in headers |
| `snowflake.timestamp.column` | String | | Timestamp column for incremental loading |
| `snowflake.incrementing.column` | String | | Incrementing column for incremental loading |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `snowflake.account` | String | Snowflake account identifier |
| `snowflake.user` | String | Snowflake username |
| `snowflake.database` | String | Database name |
| `snowflake.table` | String | Target table name |
| `topics` | String | Comma-separated list of topics to consume |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `snowflake.password` | Password | | Snowflake password |
| `snowflake.schema` | String | `PUBLIC` | Schema name |
| `snowflake.warehouse` | String | | Warehouse name |
| `snowflake.role` | String | | Role name |
| `snowflake.write.mode` | String | `insert` | Write mode: `insert`, `upsert`, `merge` |
| `snowflake.key.columns` | String | | Key columns for upsert/merge (comma-separated) |
| `snowflake.batch.size` | Int | `10000` | Batch size for bulk operations |
| `snowflake.max.retry.count` | Int | `3` | Maximum retry count for transient failures |
| `snowflake.retry.delay.ms` | Int | `1000` | Retry delay in milliseconds |
| `snowflake.auto.create.table` | Boolean | `false` | Auto-create table if not exists |
| `snowflake.stage.name` | String | | Internal stage name for bulk loading |
| `snowflake.use.snowpipe` | Boolean | `false` | Use Snowpipe for continuous loading |
| `snowflake.pipe.name` | String | | Snowpipe name |

## Record Headers

### Source Headers

| Header | Description |
|--------|-------------|
| `snowflake.account` | Account identifier |
| `snowflake.database` | Database name |
| `snowflake.schema` | Schema name |
| `snowflake.table` | Table name |
| `snowflake.warehouse` | Warehouse name |
| `snowflake.stream.name` | Stream name (stream mode only) |
| `snowflake.action.type` | CDC action type: `INSERT`, `DELETE` |
| `snowflake.row.id` | Row identifier from stream |
| `snowflake.timestamp` | Record timestamp |

## Examples

### Basic Source (Table Mode)

```json
{
  "name": "snowflake-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Snowflake.SnowflakeSourceConnector",
  "snowflake.account": "abc12345.us-east-1",
  "snowflake.user": "Surgewave_USER",
  "snowflake.password": "${secrets:snowflake-password}",
  "snowflake.database": "ANALYTICS",
  "snowflake.schema": "PUBLIC",
  "snowflake.warehouse": "COMPUTE_WH",
  "snowflake.table": "ORDERS",
  "snowflake.mode": "table",
  "snowflake.timestamp.column": "updated_at"
}
```

### CDC Source (Stream Mode)

```json
{
  "name": "snowflake-cdc-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Snowflake.SnowflakeSourceConnector",
  "snowflake.account": "abc12345.us-east-1",
  "snowflake.user": "Surgewave_USER",
  "snowflake.password": "${secrets:snowflake-password}",
  "snowflake.database": "ANALYTICS",
  "snowflake.warehouse": "COMPUTE_WH",
  "snowflake.table": "CUSTOMERS",
  "snowflake.mode": "stream",
  "snowflake.stream.name": "CUSTOMERS_STREAM",
  "snowflake.poll.interval.ms": 1000
}
```

### Query Mode Source

```json
{
  "name": "snowflake-query-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Snowflake.SnowflakeSourceConnector",
  "snowflake.account": "abc12345.us-east-1",
  "snowflake.user": "Surgewave_USER",
  "snowflake.password": "${secrets:snowflake-password}",
  "snowflake.database": "ANALYTICS",
  "snowflake.warehouse": "COMPUTE_WH",
  "snowflake.mode": "query",
  "snowflake.query": "SELECT o.*, c.name AS customer_name FROM ORDERS o JOIN CUSTOMERS c ON o.customer_id = c.id WHERE o.status = 'pending'"
}
```

### Basic Sink (Insert Mode)

```json
{
  "name": "snowflake-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Snowflake.SnowflakeSinkConnector",
  "snowflake.account": "abc12345.us-east-1",
  "snowflake.user": "Surgewave_USER",
  "snowflake.password": "${secrets:snowflake-password}",
  "snowflake.database": "WAREHOUSE",
  "snowflake.schema": "RAW",
  "snowflake.warehouse": "LOAD_WH",
  "snowflake.table": "EVENTS",
  "topics": "events",
  "snowflake.auto.create.table": true
}
```

### Upsert Sink

```json
{
  "name": "snowflake-upsert-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Snowflake.SnowflakeSinkConnector",
  "snowflake.account": "abc12345.us-east-1",
  "snowflake.user": "Surgewave_USER",
  "snowflake.password": "${secrets:snowflake-password}",
  "snowflake.database": "WAREHOUSE",
  "snowflake.warehouse": "LOAD_WH",
  "snowflake.table": "USERS",
  "topics": "users",
  "snowflake.write.mode": "upsert",
  "snowflake.key.columns": "id",
  "snowflake.batch.size": 5000
}
```

### Key-Pair Authentication

```json
{
  "name": "snowflake-keypair",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Snowflake.SnowflakeSourceConnector",
  "snowflake.account": "abc12345.us-east-1",
  "snowflake.user": "Surgewave_SERVICE",
  "snowflake.authenticator": "jwt",
  "snowflake.private.key.file": "/secrets/snowflake-key.p8",
  "snowflake.private.key.passphrase": "${secrets:key-passphrase}",
  "snowflake.database": "ANALYTICS",
  "snowflake.table": "DATA"
}
```

## Snowflake Streams (CDC)

When using stream mode, the connector leverages Snowflake Streams for Change Data Capture:

1. **Stream Creation**: If the specified stream doesn't exist, it's automatically created on the source table
2. **Change Tracking**: The stream captures INSERT, UPDATE, and DELETE operations
3. **Metadata Columns**: Records include `METADATA$ACTION`, `METADATA$ISUPDATE`, and `METADATA$ROW_ID`
4. **Exactly-Once**: After consuming changes, the stream is automatically advanced

### Stream Metadata

| Column | Description |
|--------|-------------|
| `METADATA$ACTION` | `INSERT` or `DELETE` |
| `METADATA$ISUPDATE` | `TRUE` if the row is part of an UPDATE operation |
| `METADATA$ROW_ID` | Unique row identifier |

## Performance Considerations

- **Warehouse Sizing**: Use appropriately sized warehouses for your workload
- **Batch Size**: Larger batches improve throughput but increase latency
- **Multi-Cluster Warehouses**: Consider for high-concurrency scenarios
- **Clustering Keys**: Ensure tables have appropriate clustering for query performance
- **Micro-Partitions**: Snowflake automatically optimizes storage and query performance

## Limitations

- Snowpipe integration is configured but not fully implemented
- Stage-based bulk loading requires manual stage setup
- Maximum rows per poll is limited by Snowflake query result set size

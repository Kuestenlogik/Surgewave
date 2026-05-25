# SQL Query Engine

Query Surgewave topics using SQL with windowed aggregations.

## Overview

Surgewave includes a built-in SQL query engine that treats topics as tables. You can run ad-hoc queries against live topic data, create continuous queries for real-time materialized views, and perform windowed aggregations -- all without deploying a separate service like ksqlDB.

Key capabilities:

- **Topics as tables**: Each topic is a SQL table with JSON fields as columns
- **Metadata columns**: `_offset`, `_partition`, `_timestamp`, `_key` are automatically available
- **Windowed aggregations**: TUMBLE, HOP, and SESSION windows for time-based analytics
- **Continuous queries**: Long-running queries that periodically re-evaluate
- **REST API**: Execute queries and manage continuous queries via HTTP
- **Control UI integration**: SQL editor page with query history and result export

## Quick Start

Execute a SQL query via the REST API:

```bash
curl -X POST https://localhost:9093/api/sql/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT _key, status, amount FROM orders WHERE amount > 100 LIMIT 10"}'
```

Response:

```json
{
  "columns": ["_key", "status", "amount"],
  "rows": [
    ["ord-001", "completed", 150],
    ["ord-002", "pending", 200],
    ["ord-003", "completed", 175]
  ],
  "rowCount": 3
}
```

## SQL Syntax

### Basic Queries

Surgewave topics are referenced as table names. JSON message values are automatically deserialized into columns:

```sql
-- Select all fields from a topic
SELECT * FROM orders LIMIT 100

-- Filter and project
SELECT _key, customer_id, amount, status
FROM orders
WHERE status = 'pending' AND amount > 50
ORDER BY amount DESC

-- Aggregations
SELECT status, COUNT(*) as order_count, SUM(amount) as total
FROM orders
GROUP BY status

-- Joins across topics
SELECT o._key, o.amount, c.name
FROM orders o
JOIN customers c ON o.customer_id = c._key
```

### Metadata Columns

Every topic exposes these metadata columns automatically:

| Column | Type | Description |
|--------|------|-------------|
| `_offset` | long | Message offset within the partition |
| `_partition` | int | Partition number |
| `_timestamp` | DateTimeOffset | Message timestamp |
| `_key` | string | Message key |
| `_value` | string | Raw message value (when JSON parsing fails) |
| `_header_{name}` | string | Message header values (prefixed with `_header_`) |

### Windowed Aggregations

Surgewave supports three window types for time-based analytics:

#### Tumbling Windows

Fixed-size, non-overlapping windows:

```sql
SELECT
    sensor_id,
    COUNT(*) as reading_count,
    AVG(temperature) as avg_temp,
    MAX(temperature) as max_temp
FROM sensor_data
WINDOW TUMBLE(_timestamp, INTERVAL '5' MINUTE)
GROUP BY sensor_id
```

#### Hopping Windows

Fixed-size windows that advance by a configurable step (windows can overlap):

```sql
SELECT
    region,
    SUM(sales) as total_sales
FROM transactions
WINDOW HOP(_timestamp, INTERVAL '1' HOUR, INTERVAL '15' MINUTE)
GROUP BY region
```

The first interval is the window size, the second is the advance (hop) interval.

#### Session Windows

Dynamic windows that close after a gap of inactivity:

```sql
SELECT
    user_id,
    COUNT(*) as event_count,
    MIN(_timestamp) as session_start,
    MAX(_timestamp) as session_end
FROM user_events
WINDOW SESSION(_timestamp, INTERVAL '30' MINUTE)
GROUP BY user_id
```

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows |
| `COUNT(column)` | Count non-null values |
| `COUNT(DISTINCT column)` | Count distinct non-null values |
| `SUM(column)` | Sum numeric values |
| `AVG(column)` | Average numeric values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |

### Windowed Result Columns

Windowed queries automatically include `window_start` and `window_end` columns:

```json
{
  "columns": ["window_start", "window_end", "sensor_id", "reading_count", "avg_temp"],
  "rows": [
    ["2026-03-15T10:00:00Z", "2026-03-15T10:05:00Z", "sensor-1", 12, 22.5],
    ["2026-03-15T10:05:00Z", "2026-03-15T10:10:00Z", "sensor-1", 15, 23.1]
  ]
}
```

## REST API

### Execute One-Shot Query

```
POST /api/sql/execute
```

Request body:

```json
{
  "sql": "SELECT * FROM my_topic LIMIT 100"
}
```

Response:

```json
{
  "columns": ["_offset", "_partition", "_timestamp", "_key", "field1", "field2"],
  "rows": [
    [0, 0, "2026-03-15T10:00:00Z", "key1", "value1", 42],
    [1, 0, "2026-03-15T10:00:01Z", "key2", "value2", 99]
  ],
  "rowCount": 2
}
```

On error:

```json
{
  "error": "Parse error: Expected FROM clause"
}
```

### Create Continuous Query

```
POST /api/sql/queries
```

Request body:

```json
{
  "sql": "SELECT status, COUNT(*) FROM orders GROUP BY status",
  "name": "order-status-counts"
}
```

Response:

```json
{
  "queryId": "sq-0001",
  "name": "order-status-counts",
  "sql": "SELECT status, COUNT(*) FROM orders GROUP BY status",
  "status": "RUNNING",
  "rowsProcessed": 0,
  "createdAt": "2026-03-15T10:00:00Z"
}
```

### List Continuous Queries

```
GET /api/sql/queries
```

Response:

```json
[
  {
    "queryId": "sq-0001",
    "name": "order-status-counts",
    "sql": "SELECT status, COUNT(*) FROM orders GROUP BY status",
    "status": "RUNNING",
    "rowsProcessed": 1500,
    "createdAt": "2026-03-15T10:00:00Z"
  }
]
```

### Terminate Continuous Query

```
DELETE /api/sql/queries/{id}
```

Response:

```json
{
  "status": "TERMINATED"
}
```

## Configuration

Configure the SQL service in `appsettings.json`:

```json
{
  "Surgewave": {
    "Sql": {
      "Enabled": true,
      "MaxConcurrentQueries": 16,
      "DefaultResultLimit": 1000,
      "MaxMessagesPerQuery": 100000
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | `true` | Whether the SQL query service is enabled |
| `MaxConcurrentQueries` | int | `16` | Maximum number of concurrent continuous queries |
| `DefaultResultLimit` | int | `1000` | Default result limit if not specified in SQL |
| `MaxMessagesPerQuery` | int | `100000` | Maximum messages to read from a topic per query |

## Architecture

### SqlTopicSource

The `SqlTopicSource` bridges Surgewave's storage layer with the SQL engine. It reads messages from topic partitions, deserializes JSON values into key-value pairs, and yields rows as dictionaries. Non-JSON messages are stored in the `_value` column. Message headers are exposed as `_header_{name}` columns.

### SqlEngine

The `SqlEngine` uses a recursive-descent parser to parse SQL into an AST, then evaluates the query plan against registered topic sources. It handles SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, JOIN, and WINDOW clauses.

### SqlWindowExecutor

The `SqlWindowExecutor` implements windowed aggregation:

- **Tumbling**: Aligns windows to fixed intervals using `floor(timestamp / windowSize)`
- **Hopping**: Each row may fall into multiple overlapping windows
- **Session**: Groups events within an inactivity gap, merging overlapping sessions

### SurgewaveSqlService

The `SurgewaveSqlService` is an `IHostedService` that manages:

1. **One-shot execution**: Parses SQL, discovers table references, registers topic sources, and executes the query.
2. **Continuous queries**: Runs queries in background tasks with 5-second re-evaluation intervals.
3. **Query lifecycle**: Create, list, and terminate continuous queries with max concurrency enforcement.

## Control UI

The Surgewave Control UI provides a SQL editor at `/sql` with:

- **Query editor** with SQL syntax input
- **Broker-side execution** via the REST API
- **Running queries panel** showing active continuous queries
- **Query history** with LocalStorage persistence
- **JSON and CSV export** for query results

## Use Cases

- **Ad-hoc analytics**: Explore topic data without writing a consumer
- **Real-time dashboards**: Continuous queries powering live metrics
- **Data validation**: Check message structure and content across topics
- **Debugging**: Inspect messages by offset, key, or field values
- **Alerting**: Continuous queries detecting anomalies (e.g., error rates, thresholds)

## Next Steps

- [Kafka Streams](streams.md) - Full stream processing library
- [Kafka Connect](connect.md) - Data integration pipelines
- [Schema Registry](schema-registry.md) - Schema management

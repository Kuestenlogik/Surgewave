# InfluxDB Connector

The InfluxDB connector provides integration with InfluxDB 2.x for both reading time-series data using Flux queries and writing data using line protocol.

## Package

```
Kuestenlogik.Surgewave.Connect.InfluxDB
```

## Features

### Source Connector
- **Flux Query Support**: Execute custom Flux queries for flexible data extraction
- **Measurement Polling**: Poll specific measurements with automatic query generation
- Incremental polling with timestamp-based tracking
- Configurable time ranges (relative or absolute)
- Rich metadata in record headers (org, bucket, measurement, tags)
- Topic naming with pattern substitution

### Sink Connector
- **Line Protocol**: Efficient batch writing using InfluxDB line protocol
- **Configurable Precision**: Support for nanosecond, microsecond, millisecond, and second precision
- Dynamic measurement names from record fields
- Tag and field mapping configuration
- Retry with exponential backoff for transient failures
- Tombstone handling for deletes

## Authentication Methods

- **Token Authentication**: InfluxDB 2.x API token (required)

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `influxdb.url` | String | InfluxDB server URL (e.g., http://localhost:8086) |
| `influxdb.token` | Password | InfluxDB API token |
| `influxdb.org` | String | InfluxDB organization |
| `influxdb.bucket` | String | InfluxDB bucket |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `influxdb.measurement` | String | | Measurement name (required if no custom query) |
| `influxdb.query` | String | | Custom Flux query (overrides measurement) |
| `influxdb.topic.pattern` | String | `influxdb.${org}.${bucket}.${measurement}` | Topic naming pattern |
| `influxdb.poll.interval.ms` | Int | `10000` | Poll interval in milliseconds |
| `influxdb.max.rows.per.poll` | Int | `10000` | Maximum rows per poll |
| `influxdb.include.metadata` | Boolean | `true` | Include InfluxDB metadata in headers |
| `influxdb.time.range` | String | `-1h` | Relative time range for queries |
| `influxdb.start.time` | String | | Absolute start time (ISO 8601) |
| `influxdb.stop.time` | String | | Absolute stop time (ISO 8601) |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `influxdb.url` | String | InfluxDB server URL (e.g., http://localhost:8086) |
| `influxdb.token` | Password | InfluxDB API token |
| `influxdb.org` | String | InfluxDB organization |
| `influxdb.bucket` | String | InfluxDB bucket |
| `topics` | String | Comma-separated list of topics to consume |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `influxdb.measurement` | String | | Default measurement name |
| `influxdb.batch.size` | Int | `5000` | Batch size for bulk writes |
| `influxdb.max.retry.count` | Int | `3` | Maximum retry attempts |
| `influxdb.retry.delay.ms` | Int | `1000` | Delay between retries in milliseconds |
| `influxdb.measurement.field` | String | | Field to use as measurement name |
| `influxdb.timestamp.field` | String | | Field to use as timestamp |
| `influxdb.tag.fields` | String | | Comma-separated fields to use as tags |
| `influxdb.field.fields` | String | | Comma-separated fields to use as values (empty = all) |
| `influxdb.precision` | String | `ns` | Write precision: `ns`, `us`, `ms`, `s` |

## Record Headers

### Source Headers

| Header | Description |
|--------|-------------|
| `influxdb.org` | Organization name |
| `influxdb.bucket` | Bucket name |
| `influxdb.measurement` | Measurement name |
| `influxdb.timestamp` | Record timestamp |
| `influxdb.tags` | Tag key-value pairs (JSON) |

## Examples

### Basic Source (Measurement Mode)

```json
{
  "name": "influxdb-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.InfluxDB.InfluxDBSourceConnector",
  "influxdb.url": "http://localhost:8086",
  "influxdb.token": "${secrets:influxdb-token}",
  "influxdb.org": "my-org",
  "influxdb.bucket": "my-bucket",
  "influxdb.measurement": "cpu",
  "influxdb.time.range": "-1h"
}
```

### Custom Flux Query Source

```json
{
  "name": "influxdb-flux-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.InfluxDB.InfluxDBSourceConnector",
  "influxdb.url": "http://influxdb.example.com:8086",
  "influxdb.token": "${secrets:influxdb-token}",
  "influxdb.org": "analytics",
  "influxdb.bucket": "metrics",
  "influxdb.query": "from(bucket: \"metrics\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"temperature\" and r.location == \"datacenter-1\") |> aggregateWindow(every: 1m, fn: mean)",
  "influxdb.poll.interval.ms": 60000
}
```

### Source with Absolute Time Range

```json
{
  "name": "influxdb-historical-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.InfluxDB.InfluxDBSourceConnector",
  "influxdb.url": "http://localhost:8086",
  "influxdb.token": "${secrets:influxdb-token}",
  "influxdb.org": "my-org",
  "influxdb.bucket": "historical",
  "influxdb.measurement": "events",
  "influxdb.start.time": "2024-01-01T00:00:00Z",
  "influxdb.stop.time": "2024-12-31T23:59:59Z",
  "influxdb.max.rows.per.poll": 50000
}
```

### Basic Sink

```json
{
  "name": "influxdb-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.InfluxDB.InfluxDBSinkConnector",
  "influxdb.url": "http://localhost:8086",
  "influxdb.token": "${secrets:influxdb-token}",
  "influxdb.org": "my-org",
  "influxdb.bucket": "events",
  "influxdb.measurement": "app_events",
  "topics": "events"
}
```

### Sink with Tag Mapping

```json
{
  "name": "influxdb-tagged-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.InfluxDB.InfluxDBSinkConnector",
  "influxdb.url": "http://influxdb.example.com:8086",
  "influxdb.token": "${secrets:influxdb-token}",
  "influxdb.org": "monitoring",
  "influxdb.bucket": "metrics",
  "influxdb.measurement.field": "metric_name",
  "influxdb.timestamp.field": "event_time",
  "influxdb.tag.fields": "host,region,environment",
  "influxdb.field.fields": "value,count,sum",
  "topics": "metrics",
  "influxdb.precision": "ms"
}
```

### High-Throughput Sink

```json
{
  "name": "influxdb-high-throughput-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.InfluxDB.InfluxDBSinkConnector",
  "influxdb.url": "http://influxdb-cluster.example.com:8086",
  "influxdb.token": "${secrets:influxdb-token}",
  "influxdb.org": "iot",
  "influxdb.bucket": "sensor_data",
  "influxdb.measurement": "sensors",
  "topics": "iot-sensors",
  "influxdb.batch.size": 10000,
  "influxdb.precision": "ns",
  "influxdb.max.retry.count": 5,
  "influxdb.retry.delay.ms": 500
}
```

### Sink with Dynamic Measurement Names

```json
{
  "name": "influxdb-dynamic-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.InfluxDB.InfluxDBSinkConnector",
  "influxdb.url": "http://localhost:8086",
  "influxdb.token": "${secrets:influxdb-token}",
  "influxdb.org": "my-org",
  "influxdb.bucket": "multi-metrics",
  "influxdb.measurement.field": "type",
  "topics": "mixed-metrics",
  "influxdb.batch.size": 2000
}
```

## Write Precision

InfluxDB supports multiple timestamp precisions:

| Precision | Description | Use Case |
|-----------|-------------|----------|
| `ns` | Nanoseconds (default) | High-precision sensors, financial data |
| `us` | Microseconds | Most applications |
| `ms` | Milliseconds | Application metrics, logs |
| `s` | Seconds | Low-frequency data, aggregations |

## Flux Query Examples

### Filter by Tag

```flux
from(bucket: "metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "cpu")
  |> filter(fn: (r) => r.host == "server-01")
```

### Aggregate Data

```flux
from(bucket: "metrics")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "memory")
  |> aggregateWindow(every: 1h, fn: mean)
```

### Multiple Measurements

```flux
from(bucket: "metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement =~ /^(cpu|memory|disk)$/)
```

### Join Data

```flux
cpu = from(bucket: "metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "cpu")

mem = from(bucket: "metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "memory")

join(tables: {cpu: cpu, mem: mem}, on: ["_time", "host"])
```

## Performance Considerations

- **Batch Size**: 5000-10000 points typically optimal; larger batches may cause timeouts
- **Precision**: Use lowest precision needed to reduce storage and improve performance
- **Tags vs Fields**: Tags are indexed; use for high-cardinality dimensions
- **Time Range**: Limit time ranges in source queries to avoid memory issues
- **Cardinality**: Monitor series cardinality to prevent performance degradation
- **Flux Queries**: Complex queries may impact performance; use push-down filtering

## Limitations

- InfluxDB 1.x not supported (use Flux query API)
- No support for InfluxQL queries (Flux only)
- Maximum batch size limited by InfluxDB server configuration
- Write retries may cause duplicate data (use idempotent writes where possible)
- Time-series data only; no support for arbitrary document structures

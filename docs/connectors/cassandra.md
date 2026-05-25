# Apache Cassandra Connector

The Cassandra connector provides integration with Apache Cassandra for both reading from and writing to Cassandra tables.

## Package

```
Kuestenlogik.Surgewave.Connect.Cassandra
```

## Features

### Source Connector
- **Table Mode**: Poll data from Cassandra tables with optional incremental loading
- **Query Mode**: Execute custom CQL queries for data extraction
- Incremental polling with timestamp columns
- Automatic offset tracking for exactly-once delivery
- Partition and clustering key tracking
- Rich metadata in record headers
- DCAwareRoundRobinPolicy support for multi-datacenter clusters

### Sink Connector
- **Batch Inserts**: Efficient batch writing with configurable batch size
- **Upsert Support**: Insert or update semantics (Cassandra INSERT is idempotent)
- Configurable consistency levels
- TTL support for automatic data expiration
- Logged/Unlogged/Counter batch types
- Retry with exponential backoff for transient failures
- Tombstone handling for deletes

## Authentication Methods

- **Username/Password**: Standard Cassandra authentication
- **SSL/TLS**: Encrypted connections to the cluster

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `cassandra.contact.points` | String | Comma-separated list of Cassandra contact points |
| `cassandra.keyspace` | String | Cassandra keyspace name |
| `cassandra.table` | String | Table name (required for table mode) |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `cassandra.port` | Int | `9042` | Cassandra native transport port |
| `cassandra.datacenter` | String | | Local datacenter for DCAwareRoundRobinPolicy |
| `cassandra.username` | String | | Cassandra username |
| `cassandra.password` | Password | | Cassandra password |
| `cassandra.consistency` | String | `LOCAL_QUORUM` | Read consistency level |
| `cassandra.ssl.enabled` | Boolean | `false` | Enable SSL/TLS connection |
| `cassandra.mode` | String | `table` | Mode: `table` or `query` |
| `cassandra.query` | String | | Custom CQL query (for query mode) |
| `cassandra.topic.pattern` | String | `cassandra.${keyspace}.${table}` | Topic naming pattern |
| `cassandra.poll.interval.ms` | Int | `5000` | Poll interval in milliseconds |
| `cassandra.max.rows.per.poll` | Int | `10000` | Maximum rows per poll |
| `cassandra.include.metadata` | Boolean | `true` | Include Cassandra metadata in headers |
| `cassandra.timestamp.column` | String | | Timestamp column for incremental polling |
| `cassandra.partition.key.columns` | String | | Partition key columns (comma-separated) |
| `cassandra.clustering.key.columns` | String | | Clustering key columns (comma-separated) |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `cassandra.contact.points` | String | Comma-separated list of Cassandra contact points |
| `cassandra.keyspace` | String | Cassandra keyspace name |
| `cassandra.table` | String | Target table name |
| `topics` | String | Comma-separated list of topics to consume |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `cassandra.port` | Int | `9042` | Cassandra native transport port |
| `cassandra.datacenter` | String | | Local datacenter for DCAwareRoundRobinPolicy |
| `cassandra.username` | String | | Cassandra username |
| `cassandra.password` | Password | | Cassandra password |
| `cassandra.consistency` | String | `LOCAL_QUORUM` | Write consistency level |
| `cassandra.ssl.enabled` | Boolean | `false` | Enable SSL/TLS connection |
| `cassandra.write.mode` | String | `insert` | Write mode: `insert`, `upsert` |
| `cassandra.batch.size` | Int | `500` | Batch size for bulk operations |
| `cassandra.max.retry.count` | Int | `3` | Maximum retry attempts |
| `cassandra.retry.delay.ms` | Int | `1000` | Delay between retries in milliseconds |
| `cassandra.batch.type` | String | `unlogged` | Batch type: `logged`, `unlogged`, `counter` |
| `cassandra.ttl.seconds` | Int | `0` | TTL in seconds (0 = no TTL) |
| `cassandra.partition.key.columns` | String | | Partition key columns for deletes (comma-separated) |
| `cassandra.clustering.key.columns` | String | | Clustering key columns for deletes (comma-separated) |

## Record Headers

### Source Headers

| Header | Description |
|--------|-------------|
| `cassandra.keyspace` | Keyspace name |
| `cassandra.table` | Table name |
| `cassandra.partition.key` | Partition key values (colon-separated) |
| `cassandra.clustering.key` | Clustering key values (colon-separated) |
| `cassandra.writetime` | Write time (if available) |
| `cassandra.ttl` | TTL (if set) |
| `cassandra.timestamp` | Record timestamp |

## Examples

### Basic Source (Table Mode)

```json
{
  "name": "cassandra-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Cassandra.CassandraSourceConnector",
  "cassandra.contact.points": "cassandra-node1,cassandra-node2,cassandra-node3",
  "cassandra.datacenter": "datacenter1",
  "cassandra.keyspace": "analytics",
  "cassandra.table": "events",
  "cassandra.username": "app_user",
  "cassandra.password": "${secrets:cassandra-password}",
  "cassandra.mode": "table",
  "cassandra.timestamp.column": "created_at"
}
```

### Query Mode Source

```json
{
  "name": "cassandra-query-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Cassandra.CassandraSourceConnector",
  "cassandra.contact.points": "localhost:9042",
  "cassandra.keyspace": "analytics",
  "cassandra.mode": "query",
  "cassandra.query": "SELECT * FROM events WHERE bucket = 'current' ALLOW FILTERING",
  "cassandra.poll.interval.ms": 10000
}
```

### Incremental Source with Partition Keys

```json
{
  "name": "cassandra-incremental-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Cassandra.CassandraSourceConnector",
  "cassandra.contact.points": "cassandra-cluster.example.com",
  "cassandra.datacenter": "us-east-1",
  "cassandra.keyspace": "orders",
  "cassandra.table": "order_events",
  "cassandra.partition.key.columns": "customer_id,order_id",
  "cassandra.clustering.key.columns": "event_time",
  "cassandra.timestamp.column": "event_time",
  "cassandra.max.rows.per.poll": 5000
}
```

### Basic Sink (Batch Inserts)

```json
{
  "name": "cassandra-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Cassandra.CassandraSinkConnector",
  "cassandra.contact.points": "cassandra-node1,cassandra-node2",
  "cassandra.datacenter": "datacenter1",
  "cassandra.keyspace": "warehouse",
  "cassandra.table": "raw_events",
  "topics": "events",
  "cassandra.username": "app_user",
  "cassandra.password": "${secrets:cassandra-password}",
  "cassandra.batch.size": 1000
}
```

### Sink with TTL

```json
{
  "name": "cassandra-ttl-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Cassandra.CassandraSinkConnector",
  "cassandra.contact.points": "localhost",
  "cassandra.keyspace": "cache",
  "cassandra.table": "session_data",
  "topics": "sessions",
  "cassandra.ttl.seconds": 86400,
  "cassandra.batch.type": "unlogged",
  "cassandra.consistency": "LOCAL_ONE"
}
```

### Sink with SSL/TLS

```json
{
  "name": "cassandra-ssl-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Cassandra.CassandraSinkConnector",
  "cassandra.contact.points": "secure-cassandra.example.com",
  "cassandra.datacenter": "aws-us-east-1",
  "cassandra.keyspace": "production",
  "cassandra.table": "transactions",
  "topics": "transactions",
  "cassandra.ssl.enabled": true,
  "cassandra.username": "prod_user",
  "cassandra.password": "${secrets:prod-password}",
  "cassandra.consistency": "LOCAL_QUORUM"
}
```

### High-Throughput Sink

```json
{
  "name": "cassandra-high-throughput-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Cassandra.CassandraSinkConnector",
  "cassandra.contact.points": "node1,node2,node3,node4,node5",
  "cassandra.datacenter": "datacenter1",
  "cassandra.keyspace": "metrics",
  "cassandra.table": "time_series",
  "topics": "metrics",
  "cassandra.batch.size": 2000,
  "cassandra.batch.type": "unlogged",
  "cassandra.consistency": "LOCAL_ONE",
  "cassandra.max.retry.count": 5,
  "cassandra.retry.delay.ms": 500
}
```

## Consistency Levels

Cassandra supports multiple consistency levels for balancing availability and consistency:

| Level | Description |
|-------|-------------|
| `ANY` | Write succeeds if any replica acknowledges (lowest consistency) |
| `ONE` | One replica must acknowledge |
| `TWO` | Two replicas must acknowledge |
| `THREE` | Three replicas must acknowledge |
| `QUORUM` | Majority of replicas must acknowledge |
| `ALL` | All replicas must acknowledge (highest consistency) |
| `LOCAL_QUORUM` | Majority in local datacenter (recommended) |
| `EACH_QUORUM` | Majority in each datacenter |
| `SERIAL` | For lightweight transactions |
| `LOCAL_SERIAL` | For lightweight transactions in local DC |
| `LOCAL_ONE` | One replica in local datacenter |

## Batch Types

| Type | Description |
|------|-------------|
| `logged` | Atomic batches with batchlog for recovery (slower) |
| `unlogged` | No atomicity guarantee (faster, recommended for same-partition writes) |
| `counter` | For counter column updates |

## Performance Considerations

- **Batch Size**: 500-2000 rows typically optimal; larger batches may cause timeouts
- **Unlogged Batches**: Use for same-partition writes (much faster)
- **Logged Batches**: Use when atomicity across partitions is required
- **Consistency**: LOCAL_QUORUM provides good balance; LOCAL_ONE for highest throughput
- **Datacenter**: Always specify local datacenter for optimal routing
- **Token Awareness**: TokenAwarePolicy automatically routes queries to correct nodes
- **ALLOW FILTERING**: Avoid in production; use proper partition key queries

## Limitations

- CDC (Change Data Capture) requires Cassandra 3.0+ and is not yet supported
- ALLOW FILTERING queries may cause full table scans
- Counter columns require separate counter batch type
- Lightweight transactions (LWT) may impact performance
- Maximum batch size is limited by cassandra.yaml settings

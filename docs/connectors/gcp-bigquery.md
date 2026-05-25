# Google Cloud BigQuery Connector

The BigQuery connector provides integration with Google Cloud BigQuery for both reading from and writing to BigQuery tables.

## Package

```
Kuestenlogik.Surgewave.Connect.Gcp.BigQuery
```

## Features

### Source Connector
- **Table Mode**: Poll data from BigQuery tables with optional incremental loading
- **Query Mode**: Execute custom SQL queries for data extraction
- Incremental polling with timestamp columns or partitions
- Automatic offset tracking for exactly-once delivery
- Rich metadata in record headers
- Standard SQL support

### Sink Connector
- **Streaming Inserts**: Real-time data ingestion via BigQuery Streaming API
- **Batch Loading**: Bulk data loading for high-volume scenarios
- **Append/Upsert/Overwrite**: Multiple write modes for different use cases
- Auto-create tables and datasets based on record schema
- Time partitioning and clustering support
- Retry with exponential backoff for transient failures

## Authentication Methods

- **Service Account JSON**: Inline JSON credentials
- **Service Account File**: Path to credentials file
- **Application Default Credentials**: Automatic credential discovery

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `gcp.bigquery.project.id` | String | GCP project ID |
| `gcp.bigquery.dataset` | String | BigQuery dataset name |
| `gcp.bigquery.table` | String | Table name (required for table mode) |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `gcp.bigquery.credentials.json` | Password | | Service account JSON credentials |
| `gcp.bigquery.credentials.file` | String | | Path to service account key file |
| `gcp.bigquery.location` | String | `US` | BigQuery dataset location |
| `gcp.bigquery.mode` | String | `table` | Mode: `table` or `query` |
| `gcp.bigquery.query` | String | | Custom SQL query (for query mode) |
| `gcp.bigquery.topic.pattern` | String | `bigquery.${project}.${dataset}.${table}` | Topic naming pattern |
| `gcp.bigquery.poll.interval.ms` | Int | `60000` | Poll interval in milliseconds |
| `gcp.bigquery.max.rows.per.poll` | Int | `10000` | Maximum rows per poll |
| `gcp.bigquery.include.metadata` | Boolean | `true` | Include BigQuery metadata in headers |
| `gcp.bigquery.timestamp.column` | String | | Timestamp column for incremental loading |
| `gcp.bigquery.partition.field` | String | | Partition field for incremental reads |
| `gcp.bigquery.use.standard.sql` | Boolean | `true` | Use standard SQL (vs legacy SQL) |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `gcp.bigquery.project.id` | String | GCP project ID |
| `gcp.bigquery.dataset` | String | BigQuery dataset name |
| `gcp.bigquery.table` | String | Target table name |
| `topics` | String | Comma-separated list of topics to consume |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `gcp.bigquery.credentials.json` | Password | | Service account JSON credentials |
| `gcp.bigquery.credentials.file` | String | | Path to service account key file |
| `gcp.bigquery.location` | String | `US` | BigQuery dataset location |
| `gcp.bigquery.write.mode` | String | `append` | Write mode: `append`, `upsert`, `overwrite` |
| `gcp.bigquery.batch.size` | Int | `10000` | Batch size for bulk operations |
| `gcp.bigquery.max.retry.count` | Int | `3` | Maximum retry count for transient failures |
| `gcp.bigquery.retry.delay.ms` | Int | `1000` | Retry delay in milliseconds |
| `gcp.bigquery.auto.create.table` | Boolean | `false` | Auto-create table if not exists |
| `gcp.bigquery.auto.create.dataset` | Boolean | `false` | Auto-create dataset if not exists |
| `gcp.bigquery.use.streaming` | Boolean | `true` | Use streaming inserts (vs batch load) |
| `gcp.bigquery.time.partitioning` | String | | Time partitioning field |
| `gcp.bigquery.clustering.fields` | String | | Comma-separated clustering fields |
| `gcp.bigquery.schema.update.options` | String | | Schema update options |

## Record Headers

### Source Headers

| Header | Description |
|--------|-------------|
| `bigquery.project.id` | GCP project ID |
| `bigquery.dataset` | Dataset name |
| `bigquery.table` | Table name |
| `bigquery.location` | Dataset location |
| `bigquery.partition.time` | Partition timestamp (if partitioned) |
| `bigquery.insert.id` | Insert ID for deduplication |
| `bigquery.timestamp` | Record timestamp |

## Examples

### Basic Source (Table Mode)

```json
{
  "name": "bigquery-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Gcp.BigQuery.BigQuerySourceConnector",
  "gcp.bigquery.project.id": "my-gcp-project",
  "gcp.bigquery.credentials.file": "/secrets/service-account.json",
  "gcp.bigquery.dataset": "analytics",
  "gcp.bigquery.table": "events",
  "gcp.bigquery.mode": "table",
  "gcp.bigquery.timestamp.column": "created_at"
}
```

### Query Mode Source

```json
{
  "name": "bigquery-query-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Gcp.BigQuery.BigQuerySourceConnector",
  "gcp.bigquery.project.id": "my-gcp-project",
  "gcp.bigquery.credentials.json": "${secrets:gcp-credentials}",
  "gcp.bigquery.dataset": "analytics",
  "gcp.bigquery.mode": "query",
  "gcp.bigquery.query": "SELECT * FROM `my-gcp-project.analytics.orders` WHERE status = 'pending' AND created_at > @last_timestamp",
  "gcp.bigquery.poll.interval.ms": 30000
}
```

### Incremental Source with Partitioning

```json
{
  "name": "bigquery-partitioned-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Gcp.BigQuery.BigQuerySourceConnector",
  "gcp.bigquery.project.id": "my-gcp-project",
  "gcp.bigquery.credentials.file": "/secrets/service-account.json",
  "gcp.bigquery.dataset": "warehouse",
  "gcp.bigquery.table": "transactions",
  "gcp.bigquery.partition.field": "transaction_date",
  "gcp.bigquery.max.rows.per.poll": 50000
}
```

### Basic Sink (Streaming Inserts)

```json
{
  "name": "bigquery-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Gcp.BigQuery.BigQuerySinkConnector",
  "gcp.bigquery.project.id": "my-gcp-project",
  "gcp.bigquery.credentials.file": "/secrets/service-account.json",
  "gcp.bigquery.dataset": "ingestion",
  "gcp.bigquery.table": "events",
  "topics": "events",
  "gcp.bigquery.use.streaming": true,
  "gcp.bigquery.auto.create.table": true
}
```

### Sink with Time Partitioning and Clustering

```json
{
  "name": "bigquery-partitioned-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Gcp.BigQuery.BigQuerySinkConnector",
  "gcp.bigquery.project.id": "my-gcp-project",
  "gcp.bigquery.credentials.json": "${secrets:gcp-credentials}",
  "gcp.bigquery.dataset": "analytics",
  "gcp.bigquery.table": "page_views",
  "topics": "page-views",
  "gcp.bigquery.time.partitioning": "event_timestamp",
  "gcp.bigquery.clustering.fields": "user_id,session_id",
  "gcp.bigquery.batch.size": 20000
}
```

### Batch Load Sink

```json
{
  "name": "bigquery-batch-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Gcp.BigQuery.BigQuerySinkConnector",
  "gcp.bigquery.project.id": "my-gcp-project",
  "gcp.bigquery.credentials.file": "/secrets/service-account.json",
  "gcp.bigquery.dataset": "warehouse",
  "gcp.bigquery.table": "historical_data",
  "topics": "historical",
  "gcp.bigquery.use.streaming": false,
  "gcp.bigquery.write.mode": "append",
  "gcp.bigquery.batch.size": 100000
}
```

### Upsert Sink

```json
{
  "name": "bigquery-upsert-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Gcp.BigQuery.BigQuerySinkConnector",
  "gcp.bigquery.project.id": "my-gcp-project",
  "gcp.bigquery.credentials.file": "/secrets/service-account.json",
  "gcp.bigquery.dataset": "master_data",
  "gcp.bigquery.table": "customers",
  "topics": "customers",
  "gcp.bigquery.write.mode": "upsert",
  "gcp.bigquery.batch.size": 5000
}
```

## BigQuery Write Modes

### Streaming Inserts
- Real-time data ingestion (best-effort deduplication)
- Higher cost per GB
- Data available immediately for querying
- Subject to streaming quotas

### Batch Loading
- Lower cost for large volumes
- Data available after job completion
- Better for historical data and bulk migrations
- No streaming quotas apply

## Partitioning and Clustering

### Time Partitioning
- Partition tables by DATE, DATETIME, or TIMESTAMP columns
- Improves query performance for time-range queries
- Reduces costs by scanning only relevant partitions

### Clustering
- Secondary organization within partitions
- Up to 4 clustering fields supported
- Improves filter performance on clustered columns

## Performance Considerations

- **Streaming Quotas**: Be aware of BigQuery streaming insert quotas (100,000 rows/second per project)
- **Batch Size**: Larger batches improve throughput but increase memory usage
- **Partitioning**: Use time partitioning for time-series data
- **Clustering**: Add clustering for frequently filtered columns
- **Location**: Choose location close to your data sources

## Limitations

- Streaming inserts have eventual consistency (data available within seconds)
- Batch loading jobs may take minutes to complete
- Schema changes require table recreation or schema update options
- Maximum row size is 10 MB for streaming inserts

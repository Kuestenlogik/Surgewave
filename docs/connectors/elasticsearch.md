# Elasticsearch Connector

The Elasticsearch connector enables indexing Surgewave data into Elasticsearch and reading from Elasticsearch indices.

## Overview

- **Source**: Read documents from Elasticsearch using scroll or search_after
- **Sink**: Bulk index documents with configurable strategies

**Use Cases:**
- Real-time search indexing
- Log aggregation and analysis
- Analytics data pipelines
- Full-text search synchronization

## Quick Start

### Elasticsearch Sink

Index Surgewave records into Elasticsearch:

```json
{
  "name": "es-sink",
  "config": {
    "connector.class": "ElasticsearchSinkConnector",
    "elasticsearch.url": "http://localhost:9200",
    "elasticsearch.index": "events",
    "topics": "user-events",
    "write.method": "upsert"
  }
}
```

### Elasticsearch Source

Read documents from Elasticsearch:

```json
{
  "name": "es-source",
  "config": {
    "connector.class": "ElasticsearchSourceConnector",
    "elasticsearch.url": "http://localhost:9200",
    "elasticsearch.index": "products",
    "topic": "product-catalog",
    "mode": "scroll"
  }
}
```

## Configuration Reference

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `elasticsearch.url` | string | Required | Elasticsearch URL |
| `elasticsearch.username` | string | - | Username for authentication |
| `elasticsearch.password` | password | - | Password for authentication |
| `elasticsearch.api.key` | password | - | API key authentication |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `elasticsearch.index` | string | - | Target index (or pattern) |
| `index.strategy` | string | `static` | Strategy: `static`, `topic`, `time`, `field` |
| `write.method` | string | `index` | Method: `index`, `create`, `upsert` |
| `document.id.strategy` | string | `auto` | ID: `auto`, `key`, `field`, `composite` |
| `batch.size` | int | `1000` | Documents per bulk request |
| `flush.interval.ms` | long | `1000` | Max time between flushes |
| `retry.backoff.ms` | long | `1000` | Retry backoff interval |
| `max.retries` | int | `3` | Maximum retry attempts |

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topic` | string | Required | Destination Surgewave topic |
| `elasticsearch.index` | string | Required | Source index pattern |
| `mode` | string | `scroll` | Mode: `scroll`, `search_after` |
| `query` | string | `{"match_all":{}}` | Elasticsearch query |
| `poll.interval.ms` | long | `10000` | Polling interval |
| `incremental.field` | string | - | Field for incremental reads |

## Indexing Strategies

### Static Index

All documents go to a single index:

```json
{
  "index.strategy": "static",
  "elasticsearch.index": "events"
}
```

### Topic-Based Index

Index name derived from Surgewave topic:

```json
{
  "index.strategy": "topic"
}
```

Creates indices like `user-events`, `system-events`.

### Time-Based Index

Daily/hourly indices for time-series data:

```json
{
  "index.strategy": "time",
  "elasticsearch.index": "logs-${date}"
}
```

Creates indices like `logs-2024.01.15`.

### Field-Based Index

Index name from document field:

```json
{
  "index.strategy": "field",
  "index.field": "category"
}
```

## Document ID Strategies

### Auto (Default)

Elasticsearch generates document ID:

```json
{
  "document.id.strategy": "auto"
}
```

### Key-Based

Use Surgewave record key as document ID:

```json
{
  "document.id.strategy": "key"
}
```

### Field-Based

Extract ID from document field:

```json
{
  "document.id.strategy": "field",
  "document.id.field": "userId"
}
```

### Composite

Combine multiple fields:

```json
{
  "document.id.strategy": "composite",
  "document.id.fields": "tenant,userId"
}
```

## Write Methods

### Index

Create or replace document (default):

```json
{
  "write.method": "index"
}
```

### Create

Only create new documents (fail on existing):

```json
{
  "write.method": "create"
}
```

### Upsert

Update existing or insert new:

```json
{
  "write.method": "upsert"
}
```

## Source Modes

### Scroll API

Efficient for large datasets:

```json
{
  "mode": "scroll",
  "scroll.timeout": "5m"
}
```

### Search After

For real-time pagination:

```json
{
  "mode": "search_after",
  "sort.field": "@timestamp"
}
```

### Incremental Mode

Track changes using timestamp field:

```json
{
  "mode": "search_after",
  "incremental.field": "@timestamp",
  "poll.interval.ms": "60000"
}
```

## Examples

### Real-Time Search Index

Index user data for search:

```json
{
  "name": "user-search-index",
  "config": {
    "connector.class": "ElasticsearchSinkConnector",
    "elasticsearch.url": "https://es.example.com:9200",
    "elasticsearch.username": "elastic",
    "elasticsearch.password": "secret",
    "topics": "user-updates",
    "elasticsearch.index": "users",
    "write.method": "upsert",
    "document.id.strategy": "key",
    "batch.size": "500"
  }
}
```

### Log Aggregation

Time-partitioned log indices:

```json
{
  "name": "log-indexer",
  "config": {
    "connector.class": "ElasticsearchSinkConnector",
    "elasticsearch.url": "http://localhost:9200",
    "topics": "application-logs",
    "index.strategy": "time",
    "elasticsearch.index": "logs-${date}",
    "write.method": "create",
    "batch.size": "2000",
    "flush.interval.ms": "5000"
  }
}
```

### Elasticsearch to Surgewave

Export data from Elasticsearch:

```json
{
  "name": "es-export",
  "config": {
    "connector.class": "ElasticsearchSourceConnector",
    "elasticsearch.url": "http://localhost:9200",
    "elasticsearch.index": "products-*",
    "topic": "product-export",
    "mode": "scroll",
    "query": "{\"range\":{\"updated_at\":{\"gte\":\"now-1d\"}}}"
  }
}
```

## Index Templates

Create index template before starting sink:

```json
PUT _index_template/events-template
{
  "index_patterns": ["events-*"],
  "template": {
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 1
    },
    "mappings": {
      "properties": {
        "@timestamp": { "type": "date" },
        "user_id": { "type": "keyword" },
        "event_type": { "type": "keyword" },
        "payload": { "type": "object", "enabled": false }
      }
    }
  }
}
```

## Authentication

### Basic Auth

```json
{
  "elasticsearch.url": "https://localhost:9200",
  "elasticsearch.username": "elastic",
  "elasticsearch.password": "changeme"
}
```

### API Key

```json
{
  "elasticsearch.url": "https://localhost:9200",
  "elasticsearch.api.key": "base64-encoded-api-key"
}
```

### Elastic Cloud

```json
{
  "elasticsearch.url": "https://my-deployment.es.us-east-1.aws.elastic.cloud",
  "elasticsearch.api.key": "cloud-api-key"
}
```

## Troubleshooting

### Common Issues

**Bulk Indexing Failures**
- Check index mapping compatibility
- Verify document structure matches schema
- Review Elasticsearch logs for parsing errors

**Slow Indexing**
- Increase `batch.size` for better throughput
- Reduce `number_of_replicas` during initial load
- Use bulk thread pool settings in Elasticsearch

**Version Conflicts**
- Use `write.method=index` to overwrite
- Or handle conflicts in your data pipeline

### Monitoring

```bash
# Check index stats
curl -X GET "localhost:9200/_cat/indices?v"

# Monitor bulk queue
curl -X GET "localhost:9200/_cat/thread_pool/bulk?v"

# View connector metrics
surgewave connect status es-sink
```

## See Also

- [MongoDB Connector](mongodb.md)
- [PostgreSQL CDC Connector](postgresql.md)
- [Custom Connectors](custom-connectors.md)

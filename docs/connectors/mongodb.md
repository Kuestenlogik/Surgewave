# MongoDB Connector

The MongoDB connector enables real-time change data capture and data synchronization between Surgewave and MongoDB.

## Overview

- **Source**: Capture changes via Change Streams or polling
- **Sink**: Write records to MongoDB collections with bulk operations

**Use Cases:**
- Real-time MongoDB replication
- Event sourcing from document changes
- Search index synchronization
- Cross-database data pipelines

## Quick Start

### MongoDB Change Stream Source

Capture real-time changes:

```json
{
  "name": "mongo-cdc",
  "config": {
    "connector.class": "MongoDbSourceConnector",
    "mongodb.connection.string": "mongodb://localhost:27017",
    "mongodb.database": "mydb",
    "mongodb.collection": "users",
    "topic": "mongo.users",
    "source.mode": "change_stream"
  }
}
```

### MongoDB Sink

Write records to MongoDB:

```json
{
  "name": "mongo-sink",
  "config": {
    "connector.class": "MongoDbSinkConnector",
    "mongodb.connection.string": "mongodb://localhost:27017",
    "mongodb.database": "mydb",
    "mongodb.collection": "events",
    "topics": "user-events",
    "write.mode": "upsert"
  }
}
```

## Configuration Reference

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mongodb.connection.string` | string | Required | MongoDB connection URI |
| `mongodb.database` | string | Required | Database name |
| `mongodb.collection` | string | - | Collection name (or pattern) |

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topic` | string | Required | Destination Surgewave topic |
| `source.mode` | string | `change_stream` | Mode: `change_stream`, `polling` |
| `poll.interval.ms` | long | `1000` | Polling interval (polling mode) |
| `poll.field` | string | `_id` | Field for incremental polling |
| `full.document` | string | `updateLookup` | Full document option for updates |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `write.mode` | string | `insert` | Mode: `insert`, `upsert`, `replace` |
| `document.id.strategy` | string | `auto` | ID strategy: `auto`, `key`, `field` |
| `write.concern` | string | `majority` | Write concern level |
| `batch.size` | int | `1000` | Documents per batch |

## Source Modes

### Change Streams (Recommended)

Real-time change capture using MongoDB's native Change Streams:

```json
{
  "source.mode": "change_stream",
  "full.document": "updateLookup"
}
```

**Requirements:**
- MongoDB 3.6+ (replica set or sharded cluster)
- `changeStream` privilege on watched collections

**Full Document Options:**
- `default` - Only changed fields in updates
- `updateLookup` - Full document at time of notification
- `whenAvailable` - Full document if available (MongoDB 6.0+)
- `required` - Fail if full document unavailable

### Polling Mode

For standalone MongoDB or when Change Streams aren't available:

```json
{
  "source.mode": "polling",
  "poll.field": "updatedAt",
  "poll.interval.ms": "5000"
}
```

**Poll Fields:**
- `_id` - ObjectId-based incremental
- `updatedAt` - Timestamp field
- Custom field with ordered values

## Output Format

Change Stream events use Debezium-compatible format:

```json
{
  "op": "u",
  "before": null,
  "after": {
    "_id": {"$oid": "507f1f77bcf86cd799439011"},
    "name": "John Doe",
    "email": "john@example.com"
  },
  "source": {
    "database": "mydb",
    "collection": "users",
    "resumeToken": "..."
  },
  "ts_ms": 1704067200000
}
```

### Operation Types

| Op | Description |
|----|-------------|
| `c` | Create (insert) |
| `u` | Update |
| `r` | Replace |
| `d` | Delete |

## Sink Write Modes

### Insert

Insert new documents (fails on duplicate `_id`):

```json
{
  "write.mode": "insert"
}
```

### Upsert

Insert or update based on `_id`:

```json
{
  "write.mode": "upsert",
  "document.id.strategy": "key"
}
```

### Replace

Replace entire document:

```json
{
  "write.mode": "replace"
}
```

## Document ID Strategies

### Auto (Default)

MongoDB generates new ObjectId:

```json
{
  "document.id.strategy": "auto"
}
```

### Key-Based

Use Surgewave record key as `_id`:

```json
{
  "document.id.strategy": "key"
}
```

### Field-Based

Extract `_id` from document field:

```json
{
  "document.id.strategy": "field",
  "document.id.field": "userId"
}
```

## Examples

### Database-Wide CDC

Capture changes from all collections:

```json
{
  "name": "db-wide-cdc",
  "config": {
    "connector.class": "MongoDbSourceConnector",
    "mongodb.connection.string": "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
    "mongodb.database": "production",
    "topic.pattern": "mongo.${database}.${collection}",
    "source.mode": "change_stream"
  }
}
```

### Elasticsearch Sync

Sync MongoDB to Elasticsearch via Surgewave:

```json
{
  "name": "mongo-to-es",
  "config": {
    "connector.class": "MongoDbSourceConnector",
    "mongodb.connection.string": "mongodb://localhost:27017",
    "mongodb.database": "catalog",
    "mongodb.collection": "products",
    "topic": "products-sync",
    "source.mode": "change_stream",
    "full.document": "updateLookup"
  }
}
```

### High-Throughput Sink

Bulk writes with batching:

```json
{
  "name": "bulk-writer",
  "config": {
    "connector.class": "MongoDbSinkConnector",
    "mongodb.connection.string": "mongodb://localhost:27017",
    "mongodb.database": "analytics",
    "mongodb.collection": "events",
    "topics": "user-events",
    "write.mode": "insert",
    "batch.size": "5000",
    "write.concern": "w1"
  }
}
```

## Authentication

### Username/Password

```
mongodb://user:password@host:27017/database?authSource=admin
```

### X.509 Certificate

```
mongodb://host:27017/?tls=true&tlsCertificateKeyFile=/path/to/cert.pem
```

### AWS IAM (DocumentDB)

```
mongodb://host:27017/?tls=true&tlsCAFile=/path/to/rds-ca.pem&retryWrites=false
```

## Troubleshooting

### Common Issues

**Change Stream Not Supported**
- Requires replica set or sharded cluster
- Standalone MongoDB doesn't support Change Streams
- Use `source.mode=polling` as fallback

**Resume Token Expired**
- Change Streams have limited history (oplog size)
- Connector may need full re-sync
- Increase oplog size or reduce processing lag

**Write Concern Errors**
- Lower `write.concern` for better performance
- Use `w1` instead of `majority` if durability is less critical

### Monitoring

```javascript
// Check oplog size
db.getReplicationInfo()

// Watch change stream lag
db.adminCommand({ currentOp: true, $all: true })
```

## See Also

- [PostgreSQL CDC Connector](postgresql.md)
- [Elasticsearch Connector](elasticsearch.md)
- [Custom Connectors](custom-connectors.md)

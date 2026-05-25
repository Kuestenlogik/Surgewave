# Google Cloud Firestore Connector

The Google Cloud Firestore connector enables bidirectional data flow between Surgewave/Kafka topics and Firestore document databases.

## Features

- **Real-time Listener Mode**: Listen to document changes in real-time using Firestore's snapshot listeners
- **Poll Mode**: Periodic polling with incremental updates using timestamp fields
- **Query Filtering**: Filter documents with field-based queries
- **Batch Writes**: Efficient batch operations with WriteBatch (up to 500 ops per batch)
- **Multiple Write Modes**: Set, Create, Update, and Merge operations
- **Subcollection Support**: Read from and write to nested subcollections
- **Emulator Support**: Local development with Firestore emulator

## Installation

Add the connector package to your project:

```xml
<PackageReference Include="Kuestenlogik.Surgewave.Connect.Gcp.Firestore" />
```

## Configuration

### Common Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `gcp.firestore.project.id` | String | Yes | | GCP project ID |
| `gcp.firestore.credentials.json` | Password | No | | GCP credentials JSON content |
| `gcp.firestore.credentials.file` | String | No | | Path to GCP credentials file |
| `gcp.firestore.emulator.host` | String | No | | Firestore emulator host (e.g., localhost:8080) |
| `gcp.firestore.collection` | String | Yes | | Collection path to read/write |

### Source Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `gcp.firestore.topic.pattern` | String | `firestore.${collection}` | Topic naming pattern |
| `gcp.firestore.watch.mode` | String | `listen` | Watch mode: `poll` or `listen` |
| `gcp.firestore.poll.interval.ms` | Int | `5000` | Poll interval in milliseconds |
| `gcp.firestore.max.documents.per.poll` | Int | `500` | Max documents per poll |
| `gcp.firestore.include.metadata` | Boolean | `true` | Include Firestore metadata in output |
| `gcp.firestore.query.filter` | String | | Query filter (field:op:value) |
| `gcp.firestore.order.by` | String | | Field to order by |
| `gcp.firestore.order.direction` | String | `asc` | Order direction: `asc` or `desc` |
| `gcp.firestore.timestamp.field` | String | | Timestamp field for incremental polling |

**Query Filter Operators**: `eq` (==), `neq` (!=), `gt` (>), `gte` (>=), `lt` (<), `lte` (<=)

### Sink Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `topics` | String | Yes | | Surgewave topics to consume (comma-separated) |
| `gcp.firestore.document.id.field` | String | | `id` | Field to use as document ID |
| `gcp.firestore.write.mode` | String | | `set` | Write mode: `set`, `create`, `update`, `merge` |
| `gcp.firestore.batch.size` | Int | | `500` | Batch size for bulk operations |
| `gcp.firestore.max.retry.count` | Int | | `3` | Max retry attempts |
| `gcp.firestore.retry.delay.ms` | Int | | `1000` | Retry delay in milliseconds |

**Write Modes**:
- `set`: Create or overwrite documents
- `create`: Create documents (fails if exists)
- `update`: Update existing documents (fails if not exists)
- `merge`: Create or merge with existing documents

## Usage Examples

### Source: Real-time Listener Mode

```csharp
var config = new Dictionary<string, string>
{
    ["gcp.firestore.project.id"] = "my-project",
    ["gcp.firestore.collection"] = "orders",
    ["gcp.firestore.watch.mode"] = "listen",
    ["gcp.firestore.topic.pattern"] = "firestore.orders"
};

var connector = new FirestoreSourceConnector();
connector.Start(config);
```

### Source: Poll Mode with Incremental Updates

```csharp
var config = new Dictionary<string, string>
{
    ["gcp.firestore.project.id"] = "my-project",
    ["gcp.firestore.collection"] = "orders",
    ["gcp.firestore.watch.mode"] = "poll",
    ["gcp.firestore.poll.interval.ms"] = "10000",
    ["gcp.firestore.timestamp.field"] = "updatedAt",
    ["gcp.firestore.order.by"] = "updatedAt",
    ["gcp.firestore.order.direction"] = "asc"
};

var connector = new FirestoreSourceConnector();
connector.Start(config);
```

### Source: With Query Filter

```csharp
var config = new Dictionary<string, string>
{
    ["gcp.firestore.project.id"] = "my-project",
    ["gcp.firestore.collection"] = "orders",
    ["gcp.firestore.query.filter"] = "status:eq:pending",
    ["gcp.firestore.max.documents.per.poll"] = "100"
};

var connector = new FirestoreSourceConnector();
connector.Start(config);
```

### Source: Subcollection

```csharp
var config = new Dictionary<string, string>
{
    ["gcp.firestore.project.id"] = "my-project",
    ["gcp.firestore.collection"] = "users/user123/orders",
    ["gcp.firestore.watch.mode"] = "listen"
};

var connector = new FirestoreSourceConnector();
connector.Start(config);
```

### Sink: Basic Configuration

```csharp
var config = new Dictionary<string, string>
{
    ["gcp.firestore.project.id"] = "my-project",
    ["gcp.firestore.collection"] = "processed-orders",
    ["topics"] = "orders,payments",
    ["gcp.firestore.write.mode"] = "set"
};

var connector = new FirestoreSinkConnector();
connector.Start(config);
```

### Sink: Merge Mode with Custom Document ID

```csharp
var config = new Dictionary<string, string>
{
    ["gcp.firestore.project.id"] = "my-project",
    ["gcp.firestore.collection"] = "users",
    ["topics"] = "user-updates",
    ["gcp.firestore.write.mode"] = "merge",
    ["gcp.firestore.document.id.field"] = "userId"
};

var connector = new FirestoreSinkConnector();
connector.Start(config);
```

### Local Development with Emulator

```csharp
var config = new Dictionary<string, string>
{
    ["gcp.firestore.project.id"] = "demo-project",
    ["gcp.firestore.collection"] = "test-collection",
    ["gcp.firestore.emulator.host"] = "localhost:8080"
};

var connector = new FirestoreSourceConnector();
connector.Start(config);
```

## Message Format

### Source Record Structure

```json
{
  "op": "c",
  "source": {
    "project_id": "my-project",
    "collection": "orders",
    "document_id": "abc123",
    "document_path": "orders/abc123",
    "update_time": "2024-01-15T10:30:00.0000000Z",
    "create_time": "2024-01-15T10:00:00.0000000Z"
  },
  "after": {
    "id": "abc123",
    "name": "Order 1",
    "status": "pending"
  },
  "ts_ms": 1705315800000
}
```

**Operation Types**:
- `c` - Create (document added)
- `u` - Update (document modified)
- `d` - Delete (document removed)

### Headers

| Header | Description |
|--------|-------------|
| `firestore.project.id` | GCP project ID |
| `firestore.collection` | Collection path |
| `firestore.document.id` | Document ID |
| `firestore.document.path` | Full document path |
| `firestore.update.time` | Document update timestamp |
| `firestore.create.time` | Document create timestamp |
| `firestore.change.type` | Change type (Added, Modified, Removed) |

## Data Type Mapping

| Firestore Type | JSON Representation |
|----------------|---------------------|
| Timestamp | ISO 8601 string |
| GeoPoint | `{ "lat": number, "lng": number }` |
| Reference | Document path string |
| Bytes | Base64 encoded string |
| Array | JSON array |
| Map | JSON object |

## Error Handling

- **Retry Logic**: Configurable retries with exponential backoff for transient errors
- **Batch Size Limit**: Automatically enforces Firestore's 500 operations per batch limit
- **Tombstone Records**: Null values trigger document deletion
- **Invalid JSON**: Malformed records are skipped with logging

## Performance Considerations

- **Listen Mode**: Lower latency but uses persistent connections
- **Poll Mode**: More predictable resource usage, good for large collections
- **Batch Size**: Larger batches reduce round trips but increase memory usage
- **Max Documents**: Limit documents per poll to prevent memory issues with large collections

## Authentication

The connector supports multiple authentication methods:

1. **Credentials JSON**: Provide JSON credentials directly in config
2. **Credentials File**: Path to a service account key file
3. **Application Default Credentials**: Automatic when running on GCP
4. **Emulator**: No credentials needed for local emulator

## Limitations

- Firestore batch operations are limited to 500 operations per batch
- Real-time listener mode requires persistent connections
- Subcollection queries require full path specification

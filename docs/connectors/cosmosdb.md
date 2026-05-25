# Azure Cosmos DB Connector

The Azure Cosmos DB connector enables bi-directional integration between Surgewave and Azure Cosmos DB. It consists of a source connector that captures changes using Cosmos DB Change Feed and a sink connector that writes records using bulk execution.

## Features

### Source Connector
- Change Feed-based CDC (Change Data Capture)
- Pull model for efficient polling
- Continuation token-based offset tracking
- Configurable start position (beginning, now, continuation)
- CDC-compatible output format with metadata
- Automatic offset persistence

### Sink Connector
- Multiple write modes (upsert, create, replace, delete)
- Bulk execution for high throughput
- Automatic partition key extraction
- Configurable retry behavior
- Auto-create container support
- Tombstone handling for deletes

## Configuration

### Connection Settings

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `azure.cosmosdb.connection.string` | Password | No* | | Full connection string |
| `azure.cosmosdb.endpoint` | String | No* | | Cosmos DB endpoint URL |
| `azure.cosmosdb.account.key` | Password | No* | | Account key (used with endpoint) |
| `azure.cosmosdb.database` | String | Yes | | Database name |
| `azure.cosmosdb.container` | String | Yes | | Container name |

\* Either `connection.string` or `endpoint`+`account.key` must be provided.

### Source Connector Settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `azure.cosmosdb.topic.pattern` | String | `cosmosdb.${database}.${container}` | Topic naming pattern |
| `azure.cosmosdb.changefeed.start.from` | String | `now` | Start position: `beginning`, `now`, `continuation` |
| `azure.cosmosdb.changefeed.max.items` | Int | `100` | Max items per change feed batch |
| `azure.cosmosdb.changefeed.poll.interval.ms` | Int | `500` | Poll interval in milliseconds |
| `azure.cosmosdb.lease.container` | String | | Lease container name (optional) |
| `azure.cosmosdb.lease.prefix` | String | `surgewave-connector` | Lease prefix for this instance |
| `azure.cosmosdb.include.metadata` | Boolean | `true` | Include CDC metadata in output |

### Sink Connector Settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `topics` | String | | Surgewave topics to consume (comma-separated) |
| `azure.cosmosdb.write.mode` | String | `upsert` | Write mode: `upsert`, `create`, `replace`, `delete` |
| `azure.cosmosdb.partition.key.path` | String | `/id` | Partition key path (e.g., `/partitionKey`) |
| `azure.cosmosdb.id.field` | String | `id` | Field to use as document id |
| `azure.cosmosdb.batch.size` | Int | `100` | Batch size for bulk operations |
| `azure.cosmosdb.auto.create.container` | Boolean | `false` | Auto-create container if not exists |
| `azure.cosmosdb.throughput` | Int | `400` | Throughput (RU/s) for auto-created container |
| `azure.cosmosdb.max.retry.count` | Int | `9` | Max retry count for transient failures |
| `azure.cosmosdb.max.retry.wait.time.ms` | Int | `30000` | Max retry wait time in milliseconds |

## Usage Examples

### Source Connector - Capture Changes

```json
{
  "name": "cosmosdb-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.CosmosDb.CosmosDbSourceConnector",
  "azure.cosmosdb.connection.string": "AccountEndpoint=https://myaccount.documents.azure.com:443/;AccountKey=...",
  "azure.cosmosdb.database": "mydb",
  "azure.cosmosdb.container": "mycontainer",
  "azure.cosmosdb.changefeed.start.from": "beginning",
  "azure.cosmosdb.include.metadata": "true"
}
```

### Source Connector - Using Endpoint and Key

```json
{
  "name": "cosmosdb-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.CosmosDb.CosmosDbSourceConnector",
  "azure.cosmosdb.endpoint": "https://myaccount.documents.azure.com:443/",
  "azure.cosmosdb.account.key": "your-account-key==",
  "azure.cosmosdb.database": "mydb",
  "azure.cosmosdb.container": "orders",
  "azure.cosmosdb.topic.pattern": "events.${database}.${container}",
  "azure.cosmosdb.changefeed.poll.interval.ms": "1000"
}
```

### Sink Connector - Upsert Documents

```json
{
  "name": "cosmosdb-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.CosmosDb.CosmosDbSinkConnector",
  "azure.cosmosdb.connection.string": "AccountEndpoint=https://myaccount.documents.azure.com:443/;AccountKey=...",
  "azure.cosmosdb.database": "mydb",
  "azure.cosmosdb.container": "processed",
  "topics": "input-topic",
  "azure.cosmosdb.write.mode": "upsert",
  "azure.cosmosdb.partition.key.path": "/partitionKey",
  "azure.cosmosdb.batch.size": "500"
}
```

### Sink Connector - Auto-Create Container

```json
{
  "name": "cosmosdb-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.CosmosDb.CosmosDbSinkConnector",
  "azure.cosmosdb.connection.string": "AccountEndpoint=https://myaccount.documents.azure.com:443/;AccountKey=...",
  "azure.cosmosdb.database": "mydb",
  "azure.cosmosdb.container": "new-container",
  "topics": "my-topic",
  "azure.cosmosdb.auto.create.container": "true",
  "azure.cosmosdb.throughput": "1000"
}
```

## Output Format

### Source Connector Output (with metadata)

```json
{
  "op": "c",
  "source": {
    "database": "mydb",
    "container": "mycontainer",
    "partition_key": "pk-123",
    "etag": "\"00000000-0000-0000-0000-000000000000\"",
    "timestamp": 1704067200,
    "activity_id": "00000000-0000-0000-0000-000000000000"
  },
  "after": {
    "id": "doc-1",
    "partitionKey": "pk-123",
    "name": "Example Document",
    "value": 42
  },
  "ts_ms": 1704067200000
}
```

### Source Connector Output (without metadata)

```json
{
  "data": {
    "id": "doc-1",
    "partitionKey": "pk-123",
    "name": "Example Document",
    "value": 42
  }
}
```

## Headers

The source connector adds the following headers to each record:

| Header | Description |
|--------|-------------|
| `cosmosdb.database` | Source database name |
| `cosmosdb.container` | Source container name |
| `cosmosdb.partition.key` | Document partition key |
| `cosmosdb.etag` | Document ETag |
| `cosmosdb.timestamp` | Document timestamp |

## Write Modes

The sink connector supports four write modes:

| Mode | Description |
|------|-------------|
| `upsert` | Insert new documents or update existing ones (default) |
| `create` | Only insert new documents, ignore conflicts |
| `replace` | Replace existing documents, create if not exists |
| `delete` | Delete documents based on the record value |

## Tombstone Handling

When the sink receives a record with a null/empty value (tombstone), it will:
1. Parse the key to extract the document ID
2. Attempt to delete the document from Cosmos DB
3. Silently ignore if the document doesn't exist

## Offset Tracking

The source connector tracks offsets using continuation tokens:

```json
{
  "continuation_token": "eyJfa..."
}
```

This enables exactly-once semantics when resuming from a previous position.

## Performance Considerations

### Source Connector
- Use appropriate `changefeed.max.items` based on document size
- Adjust `poll.interval.ms` based on latency requirements
- Change Feed processes all physical partitions in parallel

### Sink Connector
- Enable bulk execution (default) for high throughput
- Adjust `batch.size` based on document size and RU budget
- Configure appropriate `max.retry.count` for transient failures
- Consider container throughput when sizing operations

## Authentication

The connector supports two authentication methods:

1. **Connection String** (recommended): Full connection string with embedded credentials
2. **Endpoint + Account Key**: Separate endpoint URL and account key

For production environments, use Azure Key Vault or other secret management solutions to protect credentials.

## Error Handling

- Transient failures are retried with exponential backoff
- Rate-limited requests (HTTP 429) are automatically retried
- Invalid JSON records are skipped with a warning
- Network errors trigger retry logic

## Limitations

- Change Feed only captures creates and updates (deletes require Change Feed with all versions mode)
- Single task per connector (Change Feed handles partitions internally)
- Requires System.Text.Json serialization (Newtonsoft.Json not supported)

## Dependencies

- Microsoft.Azure.Cosmos v3.56.0+

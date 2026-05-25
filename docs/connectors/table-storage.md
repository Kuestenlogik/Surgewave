# Azure Table Storage Connector

The Azure Table Storage connector enables bi-directional integration between Surgewave and Azure Table Storage. It consists of a source connector that polls tables for entities and a sink connector that writes records using batch transactions.

## Features

### Source Connector
- Query-based polling with OData filter support
- Incremental modes: none, timestamp, rowkey
- Configurable column selection
- Offset tracking for resume capability
- Metadata-enriched output format

### Sink Connector
- Multiple write modes (upsert, insert, update, delete)
- Batch transactions (up to 100 entities per partition)
- Configurable partition and row key fields
- Auto-create table support
- Retry with exponential backoff
- Tombstone handling for deletes

## Configuration

### Connection Settings

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `azure.table.connection.string` | Password | No* | | Full connection string |
| `azure.table.account.name` | String | No* | | Storage account name |
| `azure.table.account.key` | Password | No* | | Storage account key |
| `azure.table.endpoint` | String | No | | Custom endpoint (for Azurite) |
| `azure.table.name` | String | Yes | | Table name |

\* Either `connection.string` or `account.name`+`account.key` must be provided.

### Source Connector Settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `azure.table.topic.pattern` | String | `table.${table}` | Topic naming pattern |
| `azure.table.query.filter` | String | | OData filter expression |
| `azure.table.select.columns` | String | | Comma-separated columns to select |
| `azure.table.poll.interval.ms` | Int | `5000` | Poll interval in milliseconds |
| `azure.table.max.entities.per.poll` | Int | `1000` | Maximum entities per poll |
| `azure.table.incremental.mode` | String | `none` | Mode: `none`, `timestamp`, `rowkey` |
| `azure.table.incremental.column` | String | `Timestamp` | Column for incremental tracking |
| `azure.table.include.metadata` | Boolean | `true` | Include Table Storage metadata |

### Sink Connector Settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `topics` | String | | Surgewave topics to consume (comma-separated) |
| `azure.table.write.mode` | String | `upsert` | Write mode: `upsert`, `insert`, `update`, `delete` |
| `azure.table.partition.key.field` | String | `partitionKey` | Field to use as PartitionKey |
| `azure.table.row.key.field` | String | `rowKey` | Field to use as RowKey |
| `azure.table.batch.size` | Int | `100` | Batch size (max 100 per partition) |
| `azure.table.auto.create` | Boolean | `false` | Auto-create table if not exists |
| `azure.table.max.retry.count` | Int | `3` | Max retry count for failures |
| `azure.table.retry.delay.ms` | Int | `1000` | Base retry delay in milliseconds |

## Usage Examples

### Source Connector - Poll All Entities

```json
{
  "name": "table-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.Table.TableStorageSourceConnector",
  "azure.table.connection.string": "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...;EndpointSuffix=core.windows.net",
  "azure.table.name": "customers",
  "azure.table.poll.interval.ms": "10000"
}
```

### Source Connector - Incremental by Timestamp

```json
{
  "name": "table-source-incremental",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.Table.TableStorageSourceConnector",
  "azure.table.account.name": "myaccount",
  "azure.table.account.key": "your-key==",
  "azure.table.name": "orders",
  "azure.table.incremental.mode": "timestamp",
  "azure.table.topic.pattern": "events.${table}"
}
```

### Source Connector - Filtered Query

```json
{
  "name": "table-source-filtered",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.Table.TableStorageSourceConnector",
  "azure.table.connection.string": "...",
  "azure.table.name": "logs",
  "azure.table.query.filter": "Level eq 'Error' and Timestamp gt datetime'2024-01-01T00:00:00Z'",
  "azure.table.select.columns": "PartitionKey,RowKey,Message,Level,Timestamp"
}
```

### Sink Connector - Upsert Entities

```json
{
  "name": "table-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.Table.TableStorageSinkConnector",
  "azure.table.connection.string": "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...;EndpointSuffix=core.windows.net",
  "azure.table.name": "processed",
  "topics": "input-topic",
  "azure.table.partition.key.field": "tenantId",
  "azure.table.row.key.field": "entityId"
}
```

### Sink Connector - Auto-Create Table

```json
{
  "name": "table-sink-autocreate",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.Table.TableStorageSinkConnector",
  "azure.table.connection.string": "...",
  "azure.table.name": "newtable",
  "topics": "my-topic",
  "azure.table.auto.create": "true"
}
```

### Sink Connector - With Azurite Emulator

```json
{
  "name": "table-sink-local",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Azure.Table.TableStorageSinkConnector",
  "azure.table.account.name": "devstoreaccount1",
  "azure.table.account.key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
  "azure.table.endpoint": "http://127.0.0.1:10002/devstoreaccount1",
  "azure.table.name": "testtable",
  "topics": "test-topic"
}
```

## Output Format

### Source Connector Output (with metadata)

```json
{
  "source": {
    "table": "customers",
    "partition_key": "tenant-001",
    "row_key": "customer-123",
    "timestamp": "2024-01-15T10:30:00.0000000Z",
    "etag": "W/\"datetime'2024-01-15T10%3A30%3A00.0000000Z'\""
  },
  "data": {
    "PartitionKey": "tenant-001",
    "RowKey": "customer-123",
    "Name": "John Doe",
    "Email": "john@example.com",
    "Age": 30,
    "IsActive": true
  },
  "ts_ms": 1705315800000
}
```

### Source Connector Output (without metadata)

```json
{
  "PartitionKey": "tenant-001",
  "RowKey": "customer-123",
  "Name": "John Doe",
  "Email": "john@example.com",
  "Age": 30,
  "IsActive": true
}
```

## Headers

The source connector adds the following headers to each record:

| Header | Description |
|--------|-------------|
| `table.name` | Source table name |
| `table.partition.key` | Entity PartitionKey |
| `table.row.key` | Entity RowKey |
| `table.timestamp` | Entity timestamp |
| `table.etag` | Entity ETag |

## Write Modes

The sink connector supports four write modes:

| Mode | Description |
|------|-------------|
| `upsert` | Insert or replace existing entity (default) |
| `insert` | Only insert new entities, ignore conflicts |
| `update` | Only update existing entities |
| `delete` | Delete entities based on the record value |

## Incremental Modes

The source connector supports three incremental modes:

| Mode | Description |
|------|-------------|
| `none` | Poll all entities every time (default) |
| `timestamp` | Track last Timestamp, poll newer entities |
| `rowkey` | Track last PartitionKey/RowKey, poll subsequent entities |

## Tombstone Handling

When the sink receives a record with a null/empty value (tombstone), it will:
1. Parse the key to extract PartitionKey and RowKey
2. Delete the entity from Table Storage
3. Silently ignore if the entity doesn't exist

## Key Mapping

The sink connector extracts PartitionKey and RowKey from the message:

1. **Configured fields**: Uses `partition.key.field` and `row.key.field` settings
2. **Standard names**: Falls back to `PartitionKey` and `RowKey` properties
3. **Auto-generated**: If not found, generates from record key or UUID

## Batch Operations

Table Storage limits batch operations to:
- Maximum 100 entities per batch
- All entities must have the same PartitionKey
- Total batch size must be under 4 MB

The connector automatically:
- Groups entities by PartitionKey
- Splits into batches of configured size (max 100)
- Falls back to individual operations if batch fails

## OData Filter Syntax

Common filter expressions:

```
# Equality
PartitionKey eq 'tenant-001'

# Comparison
Age gt 18

# DateTime
Timestamp gt datetime'2024-01-01T00:00:00Z'

# Logical operators
Level eq 'Error' and Timestamp gt datetime'2024-01-01T00:00:00Z'
Status eq 'Active' or Status eq 'Pending'

# String operations
Name ge 'A' and Name lt 'B'
```

## Performance Considerations

### Source Connector
- Use `select.columns` to reduce data transfer
- Use `query.filter` to limit scanned entities
- Adjust `max.entities.per.poll` based on entity size
- Consider `incremental.mode` for large tables

### Sink Connector
- Group records by PartitionKey for efficient batching
- Use `upsert` mode for best performance
- Adjust `batch.size` (max 100) for throughput
- Configure appropriate retry settings

## Authentication

The connector supports two authentication methods:

1. **Connection String** (recommended): Full connection string with embedded credentials
2. **Account Name + Key**: Separate account name and shared key

For local development, use Azurite emulator with custom endpoint.

## Error Handling

- Transient failures (429, 500, 503) are retried with backoff
- Batch failures fall back to individual operations
- Insert conflicts are ignored in `insert` mode
- Update not-found errors are ignored in `update` mode

## Limitations

- Single task per connector (no parallel table reads)
- Batch operations limited to same PartitionKey
- No real-time change notifications (polling only)
- Maximum 100 entities per batch transaction

## Dependencies

- Azure.Data.Tables v12.9.1+

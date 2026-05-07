# DynamoDB Connector

The DynamoDB connector provides both source and sink capabilities for Amazon DynamoDB integration with Surgewave.

## Overview

- **Source Connector**: Captures change data from DynamoDB Streams (INSERT, MODIFY, REMOVE events)
- **Sink Connector**: Writes records to DynamoDB tables using PutItem, UpdateItem, DeleteItem, or BatchWriteItem operations

## NuGet Packages Required

```xml
<PackageReference Include="AWSSDK.DynamoDBv2" />
<PackageReference Include="AWSSDK.DynamoDBStreams" />
```

## Source Connector (DynamoDB Streams CDC)

The source connector reads from DynamoDB Streams to capture real-time changes.

### Required Configuration

| Property | Description |
|----------|-------------|
| `aws.dynamodb.stream.arn` | ARN of the DynamoDB Stream |

### Optional Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `aws.region` | `us-east-1` | AWS region |
| `aws.access.key` | - | AWS access key (uses default credential chain if not set) |
| `aws.secret.key` | - | AWS secret key |
| `aws.endpoint` | - | Custom endpoint URL (for LocalStack testing) |
| `aws.dynamodb.table.name` | Extracted from ARN | Override table name |
| `topic.pattern` | `dynamodb.${table}` | Pattern for target topic naming |
| `aws.dynamodb.shard.iterator.type` | `LATEST` | Iterator type: `TRIM_HORIZON`, `LATEST` |
| `aws.dynamodb.poll.interval.ms` | `500` | Polling interval in milliseconds |
| `aws.dynamodb.batch.max.records` | `100` | Maximum records per poll batch |
| `aws.dynamodb.start.from.beginning` | `false` | Start from beginning (uses TRIM_HORIZON) |
| `aws.dynamodb.include.metadata` | `true` | Include full metadata in source field |

### Example Configuration

```json
{
  "name": "dynamodb-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Aws.DynamoDB.DynamoDbSourceConnector",
  "aws.dynamodb.stream.arn": "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2024-01-01T00:00:00.000",
  "aws.region": "us-east-1",
  "aws.access.key": "AKIAIOSFODNN7EXAMPLE",
  "aws.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "topic.pattern": "cdc.orders"
}
```

### Output Format (Debezium-Compatible)

The source connector produces Debezium-compatible JSON:

```json
{
  "op": "c",
  "source": {
    "table": "Orders",
    "stream_arn": "arn:aws:dynamodb:...",
    "shard_id": "shardId-00000001",
    "sequence_number": "123456789",
    "event_name": "INSERT",
    "approximate_creation_time": "2024-01-15T10:30:00Z"
  },
  "before": null,
  "after": {
    "OrderId": "12345",
    "CustomerId": "C001",
    "Amount": 99.99
  },
  "ts_ms": 1705315800000
}
```

### Operation Mapping

| DynamoDB Event | Debezium Op |
|----------------|-------------|
| `INSERT` | `c` (create) |
| `MODIFY` | `u` (update) |
| `REMOVE` | `d` (delete) |

### Headers

Each record includes headers:
- `dynamodb.table` - Table name
- `dynamodb.event` - Event type (INSERT/MODIFY/REMOVE)
- `dynamodb.sequence` - DynamoDB sequence number
- `dynamodb.shard` - Shard ID

## Sink Connector

The sink connector writes records to DynamoDB tables.

### Required Configuration

| Property | Description |
|----------|-------------|
| `aws.dynamodb.table.name` | Target DynamoDB table name |
| `topics` | Source topics to consume (comma-separated) |
| `aws.dynamodb.partition.key.field` | Field to use as partition key |

### Optional Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `aws.region` | `us-east-1` | AWS region |
| `aws.access.key` | - | AWS access key |
| `aws.secret.key` | - | AWS secret key |
| `aws.endpoint` | - | Custom endpoint URL |
| `aws.dynamodb.sort.key.field` | - | Field to use as sort key |
| `aws.dynamodb.write.mode` | `put` | Write mode: `put`, `insert`, `update`, `delete` |
| `aws.dynamodb.batch.size` | `25` | Batch size (max 25) |
| `aws.dynamodb.auto.create.table` | `false` | Auto-create table if not exists |
| `aws.dynamodb.billing.mode` | `PAY_PER_REQUEST` | Billing mode for auto-created table |
| `aws.dynamodb.read.capacity` | `5` | Read capacity units (PROVISIONED mode) |
| `aws.dynamodb.write.capacity` | `5` | Write capacity units (PROVISIONED mode) |

### Example Configuration

```json
{
  "name": "dynamodb-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Aws.DynamoDB.DynamoDbSinkConnector",
  "topics": "orders",
  "aws.dynamodb.table.name": "ProcessedOrders",
  "aws.region": "us-east-1",
  "aws.access.key": "AKIAIOSFODNN7EXAMPLE",
  "aws.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "aws.dynamodb.partition.key.field": "OrderId",
  "aws.dynamodb.sort.key.field": "Timestamp",
  "aws.dynamodb.write.mode": "put",
  "aws.dynamodb.batch.size": "25"
}
```

### Write Modes

| Mode | Description |
|------|-------------|
| `put` | PutItem - overwrites existing items |
| `insert` | PutItem with condition - fails if item exists |
| `update` | UpdateItem - updates specific attributes |
| `delete` | DeleteItem - removes items |

### Input Format

The sink expects JSON-formatted values:

```json
{
  "OrderId": "12345",
  "CustomerId": "C001",
  "Amount": 99.99,
  "Items": [
    { "ProductId": "P001", "Quantity": 2 }
  ]
}
```

### Type Mapping

| JSON Type | DynamoDB Type |
|-----------|---------------|
| String | S |
| Number | N |
| Boolean | BOOL |
| Null | NULL |
| Array | L |
| Object | M |

### Tombstone Handling

Null/empty values (tombstones) trigger delete operations in `put`/`insert` modes.

## LocalStack Testing

For local development with LocalStack:

```json
{
  "aws.endpoint": "http://localhost:4566",
  "aws.region": "us-east-1",
  "aws.access.key": "test",
  "aws.secret.key": "test"
}
```

## Prerequisites

### DynamoDB Streams Setup

To use the source connector, enable DynamoDB Streams on your table:

```bash
aws dynamodb update-table \
  --table-name MyTable \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

Stream view types:
- `KEYS_ONLY` - Only key attributes
- `NEW_IMAGE` - New item after modification
- `OLD_IMAGE` - Old item before modification
- `NEW_AND_OLD_IMAGES` - Both images (recommended for CDC)

### IAM Permissions

Source connector requires:
- `dynamodb:DescribeStream`
- `dynamodb:GetShardIterator`
- `dynamodb:GetRecords`

Sink connector requires:
- `dynamodb:DescribeTable`
- `dynamodb:PutItem`
- `dynamodb:UpdateItem`
- `dynamodb:DeleteItem`
- `dynamodb:BatchWriteItem`
- `dynamodb:CreateTable` (if auto-create enabled)

## Offset Management

The source connector tracks progress using DynamoDB sequence numbers per shard. On restart, it resumes from the last committed position using `AFTER_SEQUENCE_NUMBER` iterator type.

## Limitations

- BatchWriteItem limited to 25 items per request (handled automatically)
- DynamoDB Streams records are available for 24 hours
- Single task design (shards processed sequentially)
- String partition/sort keys assumed for auto-created tables

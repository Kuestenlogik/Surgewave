# AWS Kinesis Connector

The Kinesis connector provides both source and sink capabilities for Amazon Kinesis Data Streams integration with Surgewave.

## Overview

- **Source Connector**: Consumes records from Kinesis streams with automatic shard discovery and iterator management
- **Sink Connector**: Writes records to Kinesis streams using PutRecords batch API for high throughput

## NuGet Package Required

```xml
<PackageReference Include="AWSSDK.Kinesis" />
```

## Source Connector

The source connector reads from all shards in a Kinesis stream with automatic shard discovery.

### Required Configuration

| Property | Description |
|----------|-------------|
| `aws.kinesis.stream.name` | Name of the Kinesis stream to consume |

### Optional Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `aws.region` | `us-east-1` | AWS region |
| `aws.access.key` | - | AWS access key (uses default credential chain if not set) |
| `aws.secret.key` | - | AWS secret key |
| `aws.endpoint` | - | Custom endpoint URL (for LocalStack testing) |
| `aws.kinesis.topic.pattern` | `kinesis.${stream}` | Pattern for target topic naming |
| `aws.kinesis.shard.iterator.type` | `LATEST` | Iterator type: `TRIM_HORIZON`, `LATEST`, `AT_TIMESTAMP` |
| `aws.kinesis.poll.interval.ms` | `500` | Polling interval in milliseconds |
| `aws.kinesis.batch.max.records` | `100` | Maximum records per poll batch (max 10000) |
| `aws.kinesis.start.from.beginning` | `false` | Start from beginning (uses TRIM_HORIZON) |
| `aws.kinesis.start.timestamp` | - | Start timestamp (ISO 8601) for AT_TIMESTAMP iterator |
| `aws.kinesis.include.metadata` | `true` | Include full metadata in output |

### Example Configuration

```json
{
  "name": "kinesis-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Aws.Kinesis.KinesisSourceConnector",
  "aws.kinesis.stream.name": "my-stream",
  "aws.region": "us-east-1",
  "aws.access.key": "AKIAIOSFODNN7EXAMPLE",
  "aws.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "aws.kinesis.topic.pattern": "events.${stream}",
  "aws.kinesis.shard.iterator.type": "TRIM_HORIZON"
}
```

### Output Format

With `include.metadata=true` (default):

```json
{
  "data": { "field": "value" },
  "partition_key": "pk-001",
  "sequence_number": "49590338271490256608559692540925702759818932024458690562",
  "shard_id": "shardId-000000000000",
  "approximate_arrival_timestamp": "2024-01-15T10:30:00.000Z",
  "stream": "my-stream"
}
```

With `include.metadata=false`:

```json
{
  "data": { "field": "value" }
}
```

### Headers

Each record includes headers:
- `kinesis.stream` - Stream name
- `kinesis.partition.key` - Partition key
- `kinesis.sequence` - Sequence number
- `kinesis.shard` - Shard ID
- `kinesis.arrival.timestamp` - Approximate arrival timestamp

### Shard Iterator Types

| Type | Description |
|------|-------------|
| `TRIM_HORIZON` | Start from oldest record in shard |
| `LATEST` | Start from newest record in shard |
| `AT_TIMESTAMP` | Start from specified timestamp |
| `AT_SEQUENCE_NUMBER` | Start at specific sequence number (used for offset restore) |
| `AFTER_SEQUENCE_NUMBER` | Start after specific sequence number (used for offset restore) |

## Sink Connector

The sink connector writes records to Kinesis streams using batch operations.

### Required Configuration

| Property | Description |
|----------|-------------|
| `aws.kinesis.stream.name` | Target Kinesis stream name |
| `topics` | Source topics to consume (comma-separated) |

### Optional Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `aws.region` | `us-east-1` | AWS region |
| `aws.access.key` | - | AWS access key |
| `aws.secret.key` | - | AWS secret key |
| `aws.endpoint` | - | Custom endpoint URL |
| `aws.kinesis.partition.key.field` | - | Field to use as partition key (uses record key if not set) |
| `aws.kinesis.explicit.hash.key.field` | - | Field to use as explicit hash key (optional) |
| `aws.kinesis.batch.size` | `500` | Batch size for writes (max 500) |
| `aws.kinesis.retry.count` | `3` | Number of retries for failed records |
| `aws.kinesis.retry.delay.ms` | `100` | Initial delay between retries (exponential backoff) |

### Example Configuration

```json
{
  "name": "kinesis-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Aws.Kinesis.KinesisSinkConnector",
  "topics": "events",
  "aws.kinesis.stream.name": "target-stream",
  "aws.region": "us-east-1",
  "aws.access.key": "AKIAIOSFODNN7EXAMPLE",
  "aws.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "aws.kinesis.partition.key.field": "customerId",
  "aws.kinesis.batch.size": "500"
}
```

### Partition Key Selection

The partition key is selected in the following order:
1. If `partition.key.field` is set, extract from record value JSON
2. If record has a key, use the key value
3. Generate from `topic-partition-offset`

### Input Format

The sink expects JSON-formatted record values:

```json
{
  "customerId": "C001",
  "eventType": "purchase",
  "amount": 99.99
}
```

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

Create a test stream:
```bash
aws --endpoint-url=http://localhost:4566 kinesis create-stream \
  --stream-name test-stream \
  --shard-count 2
```

## IAM Permissions

### Source Connector

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:ListShards",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:DescribeStream"
      ],
      "Resource": "arn:aws:kinesis:*:*:stream/*"
    }
  ]
}
```

### Sink Connector

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:PutRecords",
        "kinesis:PutRecord",
        "kinesis:DescribeStream"
      ],
      "Resource": "arn:aws:kinesis:*:*:stream/*"
    }
  ]
}
```

## Offset Management

The source connector tracks progress using Kinesis sequence numbers per shard. On restart:
- Stored sequence numbers are loaded from offset storage
- `AFTER_SEQUENCE_NUMBER` iterator type is used to resume
- If no stored offset exists, uses configured iterator type (default: LATEST)

## Error Handling

### Source

- **ExpiredIteratorException**: Automatically re-initializes the shard iterator
- **ProvisionedThroughputExceededException**: Backs off with double poll interval

### Sink

- **Failed Records**: Retries with exponential backoff
- **Max Retries Exceeded**: Throws exception after configured retry count

## Limitations

- PutRecords API limited to 500 records per batch
- Each record limited to 1MB
- Total batch size limited to 5MB
- Kinesis data retention: 24 hours (default) to 7 days (extended)
- Single task design for simplicity (all shards processed in one task)

## Performance Considerations

- Increase `batch.max.records` for higher throughput
- Adjust `poll.interval.ms` based on stream activity
- Use `partition.key.field` for consistent shard routing
- Consider using Kinesis Enhanced Fan-Out for high-volume consumers

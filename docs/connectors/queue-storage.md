# Azure Queue Storage Connector

The Azure Queue Storage connector enables bidirectional data flow between Surgewave/Kafka topics and Azure Queue Storage queues.

## Features

- **Source Connector**: Poll messages from Azure Queue Storage and publish to Surgewave topics
- **Sink Connector**: Consume from Surgewave topics and send to Azure Queue Storage
- At-least-once delivery semantics with visibility timeout
- Commit-based message deletion for reliable processing
- Base64 encoding/decoding support
- Azurite emulator support for local development
- Configurable retry logic with exponential backoff
- Batch processing for efficient throughput

## Installation

Add the connector package to your project:

```xml
<PackageReference Include="Kuestenlogik.Surgewave.Connect.Azure.Queue" />
```

## Source Connector Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `azure.queue.connection.string` | Password | Yes* | | Azure Storage connection string |
| `azure.queue.account.name` | String | Yes* | | Storage account name |
| `azure.queue.account.key` | Password | | | Storage account key |
| `azure.queue.endpoint` | String | | | Custom endpoint (for Azurite) |
| `azure.queue.name` | String | Yes | | Queue name to consume from |
| `azure.queue.topic.pattern` | String | | `queue.${queue}` | Topic naming pattern |
| `azure.queue.poll.interval.ms` | Int | | `1000` | Poll interval in milliseconds |
| `azure.queue.max.messages.per.poll` | Int | | `32` | Max messages per poll (max 32) |
| `azure.queue.visibility.timeout.seconds` | Int | | `30` | Visibility timeout |
| `azure.queue.delete.after.read` | Boolean | | `false` | Delete immediately after read |
| `azure.queue.base64.decode` | Boolean | | `true` | Decode Base64 messages |
| `azure.queue.include.metadata` | Boolean | | `true` | Include metadata in output |

*Either connection string OR account name (with key) must be provided.

## Sink Connector Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `azure.queue.connection.string` | Password | Yes* | | Azure Storage connection string |
| `azure.queue.account.name` | String | Yes* | | Storage account name |
| `azure.queue.account.key` | Password | | | Storage account key |
| `azure.queue.endpoint` | String | | | Custom endpoint (for Azurite) |
| `azure.queue.name` | String | Yes | | Queue name to send to |
| `topics` | String | Yes | | Surgewave topics to consume (comma-separated) |
| `azure.queue.time.to.live.seconds` | Int | | `-1` | Message TTL (-1 = never expires) |
| `azure.queue.batch.size` | Int | | `32` | Batch size for sending |
| `azure.queue.base64.encode` | Boolean | | `true` | Base64 encode messages |
| `azure.queue.auto.create` | Boolean | | `false` | Auto-create queue if not exists |
| `azure.queue.max.retry.count` | Int | | `3` | Max retry attempts |
| `azure.queue.retry.delay.ms` | Int | | `1000` | Retry delay in milliseconds |

*Either connection string OR account name (with key) must be provided.

## Usage Examples

### Source Connector

```csharp
var config = new Dictionary<string, string>
{
    ["azure.queue.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=myaccount;...",
    ["azure.queue.name"] = "my-queue",
    ["azure.queue.topic.pattern"] = "azure.queue.${queue}",
    ["azure.queue.max.messages.per.poll"] = "32",
    ["azure.queue.visibility.timeout.seconds"] = "60"
};

var connector = new QueueStorageSourceConnector();
connector.Start(config);
```

### Sink Connector

```csharp
var config = new Dictionary<string, string>
{
    ["azure.queue.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=myaccount;...",
    ["azure.queue.name"] = "my-queue",
    ["topics"] = "orders,payments",
    ["azure.queue.time.to.live.seconds"] = "86400",  // 24 hours
    ["azure.queue.auto.create"] = "true"
};

var connector = new QueueStorageSinkConnector();
connector.Start(config);
```

### Using Azurite Emulator

```csharp
var config = new Dictionary<string, string>
{
    ["azure.queue.endpoint"] = "http://127.0.0.1:10001/devstoreaccount1",
    ["azure.queue.name"] = "test-queue"
};
```

## Message Format

### Source Record Structure

When `azure.queue.include.metadata` is `true`:

```json
{
  "source": {
    "queue": "my-queue",
    "message_id": "abc123",
    "pop_receipt": "xyz789",
    "dequeue_count": 1,
    "inserted_on": "2024-01-15T10:30:00.0000000Z",
    "expires_on": "2024-01-22T10:30:00.0000000Z",
    "next_visible_on": "2024-01-15T10:30:30.0000000Z"
  },
  "data": { /* original message content */ },
  "ts_ms": 1705315800000
}
```

### Headers

The source connector adds the following headers to each record:

| Header | Description |
|--------|-------------|
| `queue.name` | Queue name |
| `queue.message.id` | Message ID |
| `queue.pop.receipt` | Pop receipt for deletion |
| `queue.dequeue.count` | Number of times dequeued |
| `queue.inserted.on` | Insertion timestamp |
| `queue.expires.on` | Expiration timestamp |
| `queue.next.visible.on` | Next visibility timestamp |

## Delivery Semantics

### Source Connector

The source connector uses visibility timeout for at-least-once delivery:

1. Messages are received with a visibility timeout
2. Messages become invisible to other consumers
3. On successful commit, messages are deleted
4. If processing fails, messages become visible again after timeout

Use `azure.queue.delete.after.read=true` for at-most-once semantics (fire-and-forget).

### Sink Connector

The sink connector provides at-least-once delivery:

1. Messages are sent with configurable retry logic
2. Transient failures (429, 500, 503) trigger retries
3. Retry delay increases with each attempt

## Performance Considerations

- **Batch Size**: Azure Queue Storage limits to 32 messages per receive call
- **Poll Interval**: Balance between latency and API costs
- **Visibility Timeout**: Set higher than expected processing time
- **TTL**: Use appropriate TTL to avoid queue buildup
- **Base64**: Disable if not needed to reduce message size

## Error Handling

- Transient errors (rate limiting, server errors) are automatically retried
- Non-existent queues return empty results (source) or can be auto-created (sink)
- Expired visibility timeouts cause messages to reappear
- Invalid pop receipts during commit are silently ignored

# Dead Letter Queue

Broker-level DLQ with native Nack protocol support.

## Overview

Surgewave provides a broker-native Dead Letter Queue (DLQ) that automatically routes messages to a dedicated DLQ topic after a configurable number of failed processing attempts. Unlike Kafka, where DLQ handling must be implemented entirely on the client side, Surgewave's DLQ is a first-class broker feature with:

- **Native Nack operation** (`0x0204`): A dedicated protocol operation for negative acknowledgments
- **Per-message retry tracking**: The broker tracks how many times each message has been nacked
- **Automatic DLQ topic creation**: `.DLQ` topics are created on-demand when a message exceeds max retries
- **Backoff-based re-delivery**: Failed messages are re-delivered with configurable backoff delays
- **Client `NackAsync()` method**: A single API call to signal processing failure

## How It Works

```
Consumer                    Broker (DlqManager)                DLQ Topic
   |                              |                              |
   |<----- Fetch message ---------|                              |
   | (processing fails)           |                              |
   |--- NackAsync(topic,p,off) -->|                              |
   |                              | retryCount++ (1/3)           |
   |                              | schedule re-delivery         |
   |                              |   with backoff               |
   |                              |                              |
   |<----- Re-deliver message ----|                              |
   | (processing fails again)     |                              |
   |--- NackAsync(topic,p,off) -->|                              |
   |                              | retryCount++ (2/3)           |
   |                              | schedule re-delivery         |
   |                              |                              |
   |<----- Re-deliver message ----|                              |
   | (processing fails again)     |                              |
   |--- NackAsync(topic,p,off) -->|                              |
   |                              | retryCount++ (3/3)           |
   |                              | MAX RETRIES EXCEEDED         |
   |                              |--- Route to DLQ ------------>|
   |                              |   (auto-create topic)        |
   |                              |   (add surgewave-retry-count)    |
```

## Configuration

### Broker Configuration

Enable the DLQ manager in `appsettings.json`:

```json
{
  "Surgewave": {
    "Dlq": {
      "Enabled": true,
      "MaxRetries": 3,
      "RetryBackoffMs": 1000,
      "TopicSuffix": ".DLQ",
      "CleanupIntervalMs": 60000,
      "EntryMaxAgeMs": 300000
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | `false` | Enable broker-level DLQ management |
| `MaxRetries` | int | `3` | Maximum retry attempts before routing to DLQ |
| `RetryBackoffMs` | long | `1000` | Backoff delay between retries (multiplied by retry count) |
| `TopicSuffix` | string | `".DLQ"` | Suffix appended to original topic name for the DLQ topic |
| `CleanupIntervalMs` | int | `60000` | Interval for cleaning up stale retry tracking entries |
| `EntryMaxAgeMs` | long | `300000` | Maximum age for retry tracking entries before cleanup (5 minutes) |

### DLQ Topic Naming

DLQ topics are named by appending the configured suffix to the original topic name:

| Original Topic | DLQ Topic |
|----------------|-----------|
| `orders` | `orders.DLQ` |
| `user-events` | `user-events.DLQ` |
| `sensor-data` | `sensor-data.DLQ` |

DLQ topics are auto-created with:
- 1 partition
- 1 replication factor
- `cleanup.policy=delete`
- `retention.ms=604800000` (7 days)

## Usage

### Nacking Messages (Client)

Use the `NackAsync()` method on the Surgewave native client to signal that a message could not be processed:

```csharp
using var client = await SurgewaveClient.Create("localhost:9092")
    .UseSurgewaveProtocol()
    .BuildAsync();

// Nack a message after a processing failure
var (routedToDlq, retryCount) = await client.Messaging.NackAsync(
    topic: "orders",
    partition: 0,
    offset: 42);

if (routedToDlq)
{
    Console.WriteLine($"Message routed to DLQ after {retryCount} retries");
}
else
{
    Console.WriteLine($"Message scheduled for re-delivery (retry {retryCount})");
}
```

### Consumer Pattern with DLQ

A typical consumer loop with nack-based error handling:

```csharp
using var consumer = await SurgewaveClient.Create("localhost:9092")
    .UseSurgewaveProtocol()
    .BuildConsumerAsync<string, string>(new ConsumerOptions
    {
        GroupId = "order-processor",
        Topics = ["orders"]
    });

await foreach (var result in consumer.ConsumeAsync())
{
    try
    {
        await ProcessOrder(result.Value);
        await consumer.CommitAsync(result);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Processing failed: {ex.Message}");

        // Nack the message -- broker handles retry/DLQ routing
        var (routedToDlq, _) = await consumer.NackAsync(
            result.Topic, result.Partition, result.Offset);

        if (routedToDlq)
        {
            // Message is now in orders.DLQ -- commit to advance past it
            await consumer.CommitAsync(result);
        }
        // If not routed to DLQ, the message will be re-delivered with backoff
    }
}
```

### Monitoring DLQ Topics

Use the CLI to inspect DLQ topics:

```bash
# List DLQ topics
surgewave topics list --filter "*.DLQ"

# Consume from DLQ for inspection
surgewave consume orders.DLQ --from-beginning --max-messages 10

# Check DLQ topic details
surgewave topics describe orders.DLQ
```

## Architecture

### DlqManager

The `DlqManager` is a broker-side component that:

1. **Tracks retry state** per message using a `ConcurrentDictionary<(Topic, Partition, Offset), RetryEntry>`.
2. **Handles nacks** by incrementing the retry count and either scheduling re-delivery or routing to DLQ.
3. **Computes backoff**: The delay before re-delivery is `RetryBackoffMs * retryCount` (linear backoff).
4. **Routes to DLQ** by reading the original message from the source partition, adding a `surgewave-retry-count` header, and appending it to the DLQ topic.
5. **Periodic cleanup**: A timer removes stale retry entries older than `EntryMaxAgeMs`.

### Nack Protocol Operation

The native Nack operation uses opcode `0x0204`:

| Field | Type | Description |
|-------|------|-------------|
| Topic | string | Source topic name |
| Partition | int32 | Source partition |
| Offset | int64 | Message offset to nack |

The response includes:

| Field | Type | Description |
|-------|------|-------------|
| RoutedToDlq | bool | Whether the message was sent to DLQ |
| RetryCount | int32 | Current retry count for the message |

### Retry Header

Messages re-delivered after a nack carry a `surgewave-retry-count` header indicating the current retry attempt number. This allows consumers to implement retry-aware logic:

```csharp
var retryCount = result.Headers.TryGetValue("surgewave-retry-count", out var countBytes)
    ? BitConverter.ToInt32(countBytes)
    : 0;

if (retryCount > 0)
{
    Console.WriteLine($"Processing retry #{retryCount}");
}
```

## Control UI Integration

The Surgewave Control UI includes a DLQ Management Dashboard that provides:

- **Auto-detection** of DLQ topics by naming convention (`*.DLQ`, `*.dead-letter`, `*-dlq`, `*.error`)
- **Error distribution analysis** with grouped error types
- **Message browser** with DLQ-specific metadata (original topic, partition, offset, error reason, retry count)
- **Single and batch message replay** to source topics with replay headers
- **DLQ purge** for clearing processed dead letters

## Next Steps

- [Per-Message TTL](ttl.md) - Automatic message expiration
- [Log Compaction](compaction.md) - Key-based deduplication
- [Quotas](quotas.md) - Rate limiting and throttling

# Producer API

Complete guide to producing messages with Surgewave.

## Quick Start

The recommended way to produce messages is using the typed `SurgewaveProducer<TKey, TValue>`:

```csharp
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
});

await producer.ProduceAsync("orders", "order-123", new Order { Id = 123, Amount = 99.99m });
```

## SurgewaveProducer Configuration

### Basic Setup

```csharp
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.ClientId = "my-producer";
    options.BatchSize = 100;
    options.LingerMs = 5;
    options.RequestTimeoutMs = 30000;
});
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `BootstrapServers` | string | Required | Broker address (host:port) |
| `ClientId` | string | null | Client identifier for logging |
| `KeySerializer` | ISerializer | Auto | Key serializer (auto-detected by type) |
| `ValueSerializer` | ISerializer | Auto | Value serializer (auto-detected by type) |
| `BatchSize` | int | 100 | Maximum messages per batch |
| `LingerMs` | int | 5 | Wait time before sending batch |
| `RequestTimeoutMs` | int | 30000 | Request timeout |
| `Transport` | SurgewaveTransportType | Auto | Transport type (Auto, Tcp, SharedMemory) |

### Transport Selection

```csharp
await using var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.Transport = SurgewaveTransportType.SharedMemory; // ultra-low latency (target) for local
});
```

| Transport | Use Case | Latency |
|-----------|----------|---------|
| `Auto` | Default - uses SharedMemory for localhost, TCP otherwise | Varies |
| `Tcp` | Remote brokers | low (target) |
| `SharedMemory` | Same-machine IPC | ultra-low (target) |

## Serializers

Built-in serializers are auto-detected based on type:

| Type | Serializer |
|------|------------|
| `string` | UTF-8 string |
| `byte[]` | Pass-through |
| `int` | Big-endian 32-bit |
| `long` | Big-endian 64-bit |
| `Guid` | 16-byte binary |
| Complex types | JSON (System.Text.Json) |

### Custom Serializer

```csharp
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.ValueSerializer = Serializers.Json<Order>();  // Explicit JSON
});
```

## Producing Messages

### Simple Produce

```csharp
var offset = await producer.ProduceAsync("orders", "key-123", order);
Console.WriteLine($"Produced at offset {offset}");
```

### To Specific Partition

```csharp
var offset = await producer.ProduceAsync(
    topic: "orders",
    partition: 2,
    key: "key-123",
    value: order);
```

### Flush Pending Messages

```csharp
await producer.FlushAsync();
```

## Low-Level API

For advanced scenarios, use the fluent `SurgewaveNativeClient` API:

### Single Message

```csharp
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

await client.Messaging.Send("my-topic")
    .WithValue("Hello, Surgewave!")
    .ExecuteAsync();
```

### With Key and Headers

```csharp
await client.Messaging.Send("events")
    .WithKey(Encoding.UTF8.GetBytes("event-456"))
    .WithValue(eventData)
    .WithHeader("correlation-id", correlationId)
    .WithHeader("source", "order-service")
    .ExecuteAsync();
```

### Batch Produce

```csharp
await client.Messaging.Send("orders")
    .WithKey("key1").WithValue(value1)
    .And("key2", value2)
    .And("key3", value3)
    .SendAllAsync();
```

## Partitioning

### Explicit Partition

```csharp
await client.Messaging.Send("events")
    .ToPartition(0)
    .WithValue(data)
    .ExecuteAsync();
```

### By Key (Default)

Messages with the same key go to the same partition:

```csharp
await client.Messaging.Send("orders")
    .WithKey(customerId)  // Same customer -> same partition
    .ToPartition(Partitioner.ByKey)
    .WithValue(orderData)
    .ExecuteAsync();
```

### Round-Robin

Distribute evenly across partitions:

```csharp
await client.Messaging.Send("logs")
    .ToPartition(Partitioner.RoundRobin)
    .WithValue(logData)
    .ExecuteAsync();
```

### Sticky Partitioner

Batch messages to the same partition for efficiency:

```csharp
await client.Messaging.Send("events")
    .ToPartition(Partitioner.Sticky)
    .WithValue(data)
    .ExecuteAsync();
```

### Custom Partitioner

```csharp
public class GeoPartitioner : IPartitioner
{
    public int GetPartition(string topic, byte[]? key, int partitionCount)
    {
        var region = ExtractRegion(key);
        return region switch
        {
            "us" => 0,
            "eu" => 1,
            "asia" => 2,
            _ => 0
        };
    }
}

await client.Messaging.Send("geo-events")
    .ToPartition(new GeoPartitioner())
    .WithKey(regionKey)
    .WithValue(data)
    .ExecuteAsync();
```

## Send Presets

Presets configure batching, compression, retries, and timeouts for common scenarios:

| Preset | Compression | Linger | Batch Size | Retries | Use Case |
|--------|-------------|--------|------------|---------|----------|
| `Default` | None | 0ms | 1000 | 0 | General purpose |
| `LowLatency` | None | 0ms | 1 | 1 | Real-time, minimal delay |
| `HighThroughput` | LZ4 | 5ms | 10000 | 0 | Bulk loads, max msgs/sec |
| `Reliable` | Zstd | - | - | 5 | Critical data, retries |
| `HighCompression` | Zstd (level 9) | - | - | 0 | Storage optimization |

```csharp
// Low latency - immediate send, no batching
await client.Messaging.Send("realtime")
    .UsePreset(SendPreset.LowLatency)
    .WithValue(data)
    .ExecuteAsync();

// High throughput - batching and compression
await client.Messaging.Send("bulk")
    .UsePreset(SendPreset.HighThroughput)
    .WithValue(data)
    .ExecuteAsync();

// Reliable - retries with exponential backoff
await client.Messaging.Send("critical")
    .UsePreset(SendPreset.Reliable)
    .WithValue(data)
    .ExecuteAsync();
```

### Custom Preset

```csharp
var customPreset = client.Messaging.CreatePreset()
    .WithCompression(CompressionType.Lz4)
    .WithCompressionLevel(3)
    .WithLingerTime(TimeSpan.FromMilliseconds(10))
    .WithMaxBatchSize(5000)
    .WithRetry(attempts: 3, backoff: TimeSpan.FromMilliseconds(100))
    .Build();

await client.Messaging.Send("custom")
    .UsePreset(customPreset)
    .WithValue(data)
    .ExecuteAsync();
```

## Compression

| Codec | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| `None` | Fastest | 1x | Low CPU, small messages |
| `Lz4` | Very Fast | 2-3x | General purpose (recommended) |
| `Zstd` | Fast | 3-5x | High compression needs |
| `Gzip` | Slow | 2-4x | Kafka compatibility |
| `Snappy` | Fast | 2-3x | Kafka compatibility |

## Error Handling

```csharp
try
{
    await producer.ProduceAsync("topic", key, value);
}
catch (SurgewaveException ex)
{
    switch (ex.ErrorCode)
    {
        case SurgewaveErrorCode.TopicNotFound:
            // Create topic or handle missing
            break;
        case SurgewaveErrorCode.NotLeader:
            // Retry - leader changed
            break;
        case SurgewaveErrorCode.MessageTooLarge:
            // Reduce message size
            break;
        default:
            throw;
    }
}
```

## Transactions

Use the native client's transaction operations for atomic writes:

```csharp
// Initialize transactional producer
var producerId = await client.Transactions.InitProducerIdAsync("my-transactional-id");

// Produce within a transaction
await client.Messaging.Send("orders").WithValue(order).ExecuteAsync();
await client.Messaging.Send("inventory").WithValue(update).ExecuteAsync();

// Commit the transaction
await client.Transactions.EndTransactionAsync(producerId, commit: true);
```

## Performance Tips

1. **Use SurgewaveProducer** - The typed producer handles batching and serialization efficiently
2. **Batch messages** - Use `HighThroughput` preset for bulk operations
3. **Use LZ4 compression** - Best speed/ratio tradeoff
4. **Reuse producer** - Create once, use throughout application lifetime
5. **Use SharedMemory** - For same-machine scenarios, get ultra-low latency (target)
6. **Async all the way** - Never block on produce operations

## Next Steps

- [Consumer API](consumer.md) - Consume messages
- [Transactions](../features/transactions.md) - Exactly-once semantics
- [Performance](../performance/tuning.md) - Optimization guide

# Confluent.Kafka API Wrapper

The `Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka` package provides a drop-in replacement for the `Confluent.Kafka` NuGet package, enabling zero-code-change migration from Kafka to Surgewave.

## Overview

This package wraps Surgewave.Client while exposing the exact same API as `Confluent.Kafka`. You can migrate existing Kafka applications to Surgewave by simply changing a `using` statement.

### Migration Path

| Step | Package | Protocol | Description |
|------|---------|----------|-------------|
| 1 | `Confluent.Kafka` | Kafka | Original Kafka client |
| 2 | `Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka` | Auto | Surgewave wrapper (this package) |
| 3 | `Kuestenlogik.Surgewave.Client` | Native | Full Surgewave performance |

## Installation

```bash
dotnet add package Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka
```

## Quick Migration

### Before (Confluent.Kafka)

```csharp
using Confluent.Kafka;

var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(config).Build();
await producer.ProduceAsync("my-topic", new Message<string, string> { Value = "Hello" });
```

### After (Surgewave Wrapper)

```csharp
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka;  // Only change this line!

var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(config).Build();
await producer.ProduceAsync("my-topic", new Message<string, string> { Value = "Hello" });
```

## Protocol Selection

By default, the wrapper auto-detects the best protocol. You can explicitly select the protocol:

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SurgewaveProtocol = "surgewave"  // Options: "surgewave", "kafka", "auto" (default)
};
```

| Protocol | Description | Performance |
|----------|-------------|-------------|
| `auto` | Auto-detect (tries Surgewave first) | Optimal |
| `surgewave` | Force Surgewave native protocol | lower latency |
| `kafka` | Force Kafka protocol | Kafka compatible |

## Producer API

### Basic Producer

```csharp
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = "my-producer",
    Acks = Acks.All,
    EnableIdempotence = true
};

using var producer = new ProducerBuilder<string, string>(config).Build();

// Async produce
var result = await producer.ProduceAsync("orders", new Message<string, string>
{
    Key = "order-123",
    Value = """{"item": "widget", "qty": 5}"""
});

Console.WriteLine($"Delivered to partition {result.Partition} at offset {result.Offset}");

// Flush pending messages
producer.Flush(TimeSpan.FromSeconds(10));
```

### Producer with Headers

```csharp
var message = new Message<string, string>
{
    Key = "order-123",
    Value = "order data",
    Headers = new Headers
    {
        { "correlation-id", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) },
        { "source", Encoding.UTF8.GetBytes("web-api") }
    }
};

await producer.ProduceAsync("orders", message);
```

### Fire-and-Forget with Callback

```csharp
producer.Produce("events", new Message<string, string>
{
    Key = "event-1",
    Value = "data"
}, report =>
{
    if (report.Error.IsError)
        Console.WriteLine($"Delivery failed: {report.Error.Reason}");
    else
        Console.WriteLine($"Delivered to {report.Partition}@{report.Offset}");
});
```

### Producer with Error Handler

```csharp
var producer = new ProducerBuilder<string, string>(config)
    .SetErrorHandler((p, e) =>
    {
        Console.WriteLine($"Producer error: {e.Reason}");
        if (e.IsFatal)
            Environment.Exit(1);
    })
    .SetLogHandler((p, log) =>
    {
        Console.WriteLine($"[{log.Level}] {log.Message}");
    })
    .Build();
```

## Consumer API

### Basic Consumer

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "order-processors",
    ClientId = "consumer-1",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe("orders");

while (true)
{
    var result = consumer.Consume(TimeSpan.FromSeconds(1));
    if (result != null && !result.IsPartitionEOF)
    {
        Console.WriteLine($"Received: {result.Message.Key} = {result.Message.Value}");
    }
}
```

### Consumer with Manual Commit

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "order-processors",
    EnableAutoCommit = false  // Manual commit
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("orders");

while (true)
{
    var result = consumer.Consume(TimeSpan.FromSeconds(1));
    if (result != null && !result.IsPartitionEOF)
    {
        try
        {
            ProcessOrder(result.Message.Value);
            consumer.Commit(result);  // Commit after processing
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Processing failed: {ex.Message}");
            // Message will be redelivered
        }
    }
}
```

### Consumer with Handlers

```csharp
var consumer = new ConsumerBuilder<string, string>(config)
    .SetErrorHandler((c, e) =>
    {
        Console.WriteLine($"Consumer error: {e.Reason}");
    })
    .SetPartitionsAssignedHandler((c, partitions) =>
    {
        Console.WriteLine($"Assigned: {string.Join(", ", partitions)}");
    })
    .SetPartitionsRevokedHandler((c, partitions) =>
    {
        Console.WriteLine($"Revoked: {string.Join(", ", partitions)}");
        // Commit offsets before revocation
    })
    .SetOffsetsCommittedHandler((c, offsets) =>
    {
        Console.WriteLine($"Committed {offsets.Offsets.Count} offsets");
    })
    .Build();
```

### Seek and Position

```csharp
// Seek to specific offset
consumer.Seek(new TopicPartitionOffset("orders", 0, 100));

// Seek to beginning
consumer.Seek(new TopicPartitionOffset("orders", 0, Offset.Beginning));

// Get current position
var position = consumer.Position(new TopicPartition("orders", 0));
Console.WriteLine($"Current position: {position}");
```

## Admin Client

### Create Topics

```csharp
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka;
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Admin;

var config = new AdminClientConfig { BootstrapServers = "localhost:9092" };

using var admin = new AdminClientBuilder(config).Build();

await admin.CreateTopicsAsync(new[]
{
    new TopicSpecification
    {
        Name = "new-topic",
        NumPartitions = 6,
        ReplicationFactor = 3,
        Configs = new Dictionary<string, string>
        {
            ["retention.ms"] = "604800000",  // 7 days
            ["cleanup.policy"] = "delete"
        }
    }
});
```

### List and Describe Topics

```csharp
// Get cluster metadata
var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));

foreach (var topic in metadata.Topics)
{
    Console.WriteLine($"Topic: {topic.Topic}");
    foreach (var partition in topic.Partitions)
    {
        Console.WriteLine($"  Partition {partition.PartitionId}: Leader={partition.Leader}");
    }
}

// Describe specific topics
var descriptions = await admin.DescribeTopicsAsync(
    TopicCollection.OfTopicNames(new[] { "orders", "events" }));

foreach (var desc in descriptions.TopicDescriptions)
{
    Console.WriteLine($"{desc.Name}: {desc.Partitions.Count} partitions");
}
```

### Delete Topics

```csharp
await admin.DeleteTopicsAsync(new[] { "old-topic", "test-topic" });
```

### Consumer Group Management

```csharp
// List consumer groups
var groups = await admin.ListConsumerGroupsAsync();
foreach (var group in groups.Valid)
{
    Console.WriteLine($"Group: {group.GroupId}, State: {group.State}");
}

// Describe consumer groups
var descriptions = await admin.DescribeConsumerGroupsAsync(new[] { "order-processors" });
foreach (var desc in descriptions.ConsumerGroupDescriptions)
{
    Console.WriteLine($"Group {desc.GroupId}: {desc.Members.Count} members");
    foreach (var member in desc.Members)
    {
        Console.WriteLine($"  - {member.ClientId}: {member.Assignment?.TopicPartitions.Count ?? 0} partitions");
    }
}

// Delete consumer group
await admin.DeleteGroupsAsync(new[] { "old-group" });
```

## Serialization

### Built-in Serializers

```csharp
// String (UTF-8)
var producer = new ProducerBuilder<string, string>(config)
    .SetKeySerializer(Serializers.Utf8)
    .SetValueSerializer(Serializers.Utf8)
    .Build();

// Byte array
var binaryProducer = new ProducerBuilder<byte[], byte[]>(config)
    .SetKeySerializer(Serializers.ByteArray)
    .SetValueSerializer(Serializers.ByteArray)
    .Build();

// Numeric types
var intProducer = new ProducerBuilder<int, long>(config)
    .SetKeySerializer(Serializers.Int32)
    .SetValueSerializer(Serializers.Int64)
    .Build();

// Null keys
var nullKeyProducer = new ProducerBuilder<Null, string>(config)
    .SetKeySerializer(Serializers.Null)
    .Build();
```

### Built-in Deserializers

```csharp
var consumer = new ConsumerBuilder<string, string>(config)
    .SetKeyDeserializer(Deserializers.Utf8)
    .SetValueDeserializer(Deserializers.Utf8)
    .Build();

// Ignore keys
var ignoreKeyConsumer = new ConsumerBuilder<Ignore, string>(config)
    .SetKeyDeserializer(Deserializers.Ignore)
    .SetValueDeserializer(Deserializers.Utf8)
    .Build();
```

### Custom Serializer

```csharp
public class JsonSerializer<T> : ISerializer<T>
{
    public byte[]? Serialize(T data, SerializationContext context)
    {
        if (data is null) return null;
        return JsonSerializer.SerializeToUtf8Bytes(data);
    }
}

public class JsonDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty) return default!;
        return JsonSerializer.Deserialize<T>(data)!;
    }
}

// Usage
var producer = new ProducerBuilder<string, Order>(config)
    .SetValueSerializer(new JsonSerializer<Order>())
    .Build();
```

## Configuration Reference

### Producer Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BootstrapServers` | string | - | Broker addresses (required) |
| `ClientId` | string | - | Client identifier |
| `Acks` | Acks | Leader | Acknowledgment level |
| `LingerMs` | double | 5 | Batch wait time (ms) |
| `BatchNumMessages` | int | 10000 | Max batch messages |
| `CompressionType` | CompressionType | None | Compression codec |
| `EnableIdempotence` | bool | false | Enable idempotence |
| `TransactionalId` | string | - | Transaction ID |
| `RequestTimeoutMs` | int | 30000 | Request timeout |
| `MessageTimeoutMs` | int | 300000 | Delivery timeout |
| `SurgewaveProtocol` | string | "auto" | Protocol selection |

### Consumer Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BootstrapServers` | string | - | Broker addresses (required) |
| `GroupId` | string | - | Consumer group ID |
| `ClientId` | string | - | Client identifier |
| `AutoOffsetReset` | AutoOffsetReset | Latest | Initial offset |
| `EnableAutoCommit` | bool | true | Auto commit offsets |
| `AutoCommitIntervalMs` | int | 5000 | Commit interval |
| `SessionTimeoutMs` | int | 45000 | Session timeout |
| `MaxPollIntervalMs` | int | 300000 | Max poll interval |
| `IsolationLevel` | IsolationLevel | ReadUncommitted | Transaction isolation |
| `SurgewaveProtocol` | string | "auto" | Protocol selection |

## Error Handling

### Exception Types

```csharp
try
{
    await producer.ProduceAsync("topic", message);
}
catch (ProduceException<string, string> ex)
{
    Console.WriteLine($"Produce failed: {ex.Error.Reason}");
    Console.WriteLine($"Error code: {ex.Error.Code}");
    Console.WriteLine($"Is fatal: {ex.Error.IsFatal}");
}

try
{
    var result = consumer.Consume(TimeSpan.FromSeconds(10));
}
catch (ConsumeException ex)
{
    Console.WriteLine($"Consume failed: {ex.Error.Reason}");
}
```

### Error Codes

| Code | Description |
|------|-------------|
| `NoError` | Success |
| `Local_TimedOut` | Operation timed out |
| `Local_QueueFull` | Producer queue full |
| `UnknownTopicOrPartition` | Topic doesn't exist |
| `RequestTimedOut` | Broker request timeout |
| `LeaderNotAvailable` | Partition leader unavailable |

## Complete Example

```csharp
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka;

// Producer
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = "order-service",
    Acks = Acks.All,
    SurgewaveProtocol = "surgewave"  // Use Surgewave native protocol
};

using var producer = new ProducerBuilder<string, string>(producerConfig)
    .SetErrorHandler((p, e) => Console.WriteLine($"Error: {e.Reason}"))
    .Build();

// Produce messages
for (int i = 0; i < 100; i++)
{
    var result = await producer.ProduceAsync("orders", new Message<string, string>
    {
        Key = $"order-{i}",
        Value = $"{{\"orderId\": {i}, \"amount\": {i * 10}}}"
    });
    Console.WriteLine($"Produced order {i} to partition {result.Partition}");
}

producer.Flush(TimeSpan.FromSeconds(10));

// Consumer
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "order-processors",
    ClientId = "processor-1",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    SurgewaveProtocol = "surgewave"
};

using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
    .SetPartitionsAssignedHandler((c, p) =>
        Console.WriteLine($"Assigned: {string.Join(", ", p)}"))
    .Build();

consumer.Subscribe("orders");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var result = consumer.Consume(cts.Token);
        if (result != null && !result.IsPartitionEOF)
        {
            Console.WriteLine($"Processing: {result.Message.Key} = {result.Message.Value}");
            consumer.Commit(result);
        }
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Shutting down...");
}
finally
{
    consumer.Close();
}
```

## Comparison with Native Client

| Feature | Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka | Kuestenlogik.Surgewave.Client |
|---------|--------------------------|-----------------|
| API Style | Confluent.Kafka compatible | Surgewave native |
| Migration Effort | Zero code changes | Code rewrite |
| Performance | Good (auto protocol) | Optimal |
| Learning Curve | None (familiar API) | New API to learn |
| Features | Confluent.Kafka subset | Full Surgewave features |

## When to Use

**Use Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka when:**
- Migrating existing Kafka applications
- Team is familiar with Confluent.Kafka API
- Gradual migration to Surgewave is preferred
- Need to maintain Kafka compatibility option

**Use Kuestenlogik.Surgewave.Client directly when:**
- Building new Surgewave applications
- Maximum performance is required
- Need Surgewave-specific features
- No Kafka compatibility requirement

## Limitations

The wrapper provides full API compatibility but some advanced features depend on Surgewave support:

| Feature | Status |
|---------|--------|
| Basic produce/consume | Full support |
| Consumer groups | Full support |
| Headers | Full support |
| Transactions | Partial (mock implementation) |
| Admin operations | Basic support |
| Interceptors | Not yet supported |

## Next Steps

- [Surgewave.Client Native API](dotnet.md) - For maximum performance
- [Producer Patterns](producer.md) - Advanced producer usage
- [Consumer Patterns](consumer.md) - Advanced consumer usage
- [Migration from Kafka](kafka-compat.md) - Using original Confluent.Kafka

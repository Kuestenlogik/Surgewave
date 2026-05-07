# Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka

A drop-in replacement for the `Confluent.Kafka` NuGet package that wraps Surgewave.Client, enabling zero-code-change migration from Apache Kafka to Surgewave.

## Features

- **Zero Code Changes**: Just change the `using` statement
- **Full API Compatibility**: Producer, Consumer, AdminClient with all callbacks
- **Protocol Selection**: Auto-detect, Surgewave native (much lower latency), or Kafka protocol
- **Type Compatibility**: All Confluent.Kafka types (Partition, Offset, Headers, etc.)
- **Built-in Serializers**: Utf8, ByteArray, Int32, Int64, Single, Double, Null, Ignore

## Installation

```bash
dotnet add package Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka
```

## Quick Migration

**Before (Confluent.Kafka):**
```csharp
using Confluent.Kafka;
```

**After (Surgewave Wrapper):**
```csharp
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka;
```

That's it! Your existing Kafka code works with Surgewave brokers.

## Three-Step Migration Path

| Step | Package | Protocol | Performance |
|------|---------|----------|-------------|
| 1 | `Confluent.Kafka` | Kafka | Baseline |
| 2 | `Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka` | Auto/Surgewave | Low-latency native path |
| 3 | `Kuestenlogik.Surgewave.Client` | Native | Optimal |

## Protocol Switching

By default, the wrapper auto-detects the best protocol:

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SurgewaveProtocol = "surgewave"  // Options: "surgewave", "kafka", "auto" (default)
};
```

| Protocol | Description | Latency |
|----------|-------------|---------|
| `auto` | Auto-detect (tries Surgewave first) | Optimal |
| `surgewave` | Force Surgewave native protocol | low (target) |
| `kafka` | Force Kafka protocol | Kafka-protocol baseline |

## Producer Example

```csharp
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = "my-producer",
    Acks = Acks.All
};

using var producer = new ProducerBuilder<string, string>(config)
    .SetErrorHandler((p, e) => Console.WriteLine($"Error: {e.Reason}"))
    .Build();

var result = await producer.ProduceAsync("my-topic", new Message<string, string>
{
    Key = "key",
    Value = "Hello Surgewave!",
    Headers = new Headers { { "correlation-id", Encoding.UTF8.GetBytes("123") } }
});

Console.WriteLine($"Delivered to partition {result.Partition}, offset {result.Offset}");
producer.Flush(TimeSpan.FromSeconds(10));
```

## Consumer Example

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((c, p) => Console.WriteLine($"Assigned: {string.Join(", ", p)}"))
    .SetPartitionsRevokedHandler((c, p) => Console.WriteLine($"Revoked: {string.Join(", ", p)}"))
    .Build();

consumer.Subscribe("my-topic");

while (true)
{
    var result = consumer.Consume(TimeSpan.FromSeconds(1));
    if (result != null && !result.IsPartitionEOF)
    {
        Console.WriteLine($"{result.Message.Key}: {result.Message.Value}");
    }
}
```

## Admin Client Example

```csharp
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka;
using Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Admin;

var config = new AdminClientConfig { BootstrapServers = "localhost:9092" };
using var admin = new AdminClientBuilder(config).Build();

// Create topic
await admin.CreateTopicsAsync(new[]
{
    new TopicSpecification { Name = "new-topic", NumPartitions = 3 }
});

// List topics
var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
foreach (var topic in metadata.Topics)
{
    Console.WriteLine($"Topic: {topic.Topic}, Partitions: {topic.Partitions.Count}");
}
```

## Documentation

For comprehensive documentation, see:
- [Confluent.Kafka Wrapper Guide](https://github.com/kuestenlogik/Surgewave/blob/main/docs/clients/confluent-kafka-wrapper.md)
- [Surgewave Documentation](https://github.com/kuestenlogik/Surgewave/blob/main/docs/index.md)

## License

Apache 2.0

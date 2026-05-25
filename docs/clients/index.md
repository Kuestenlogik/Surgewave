# Client Overview

Surgewave provides multiple client options for different use cases.

## Client Options

| Client | Language | Protocol | Use Case |
|--------|----------|----------|----------|
| [Surgewave.Client](dotnet.md) | .NET | Native | Native protocol; direct API access |
| [Surgewave.Confluent.Kafka](confluent-kafka-wrapper.md) | .NET | Auto | Drop-in for existing Confluent.Kafka code paths |
| [Confluent.Kafka](kafka-compat.md) | .NET | Kafka | Kafka protocol only |
| [gRPC Clients](../transport/grpc.md) | Any | gRPC | Cross-language |

## Quick Comparison

| Feature | Surgewave.Client | Surgewave.Confluent.Kafka | Confluent.Kafka |
|---------|--------------|----------------------|-----------------|
| Latency | Native (target: low) | Native or Kafka wire | Kafka wire |
| Throughput | 1.25M msg/s | Up to 1.25M msg/s* | 68K msg/s |
| Protocol | Native | Auto/Surgewave/Kafka | Kafka |
| Migration | New API | Zero code changes | Original API |

*Depends on protocol selection. Comparative head-to-head numbers will be published with the 1.0 release.

## .NET Client (Surgewave.Client)

```csharp
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Native;

// Low-level native client
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

// Produce
await client.Messaging.Send("orders")
    .WithKey("order-123")
    .WithValue(orderData)
    .ExecuteAsync();

// Typed consumer
await using var consumer = new SurgewaveConsumer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "order-processor";
});
consumer.Subscribe("orders");
var result = await consumer.ConsumeAsync();
if (result != null) ProcessOrder(result.Value);
```

## Kafka-Compatible (Confluent.Kafka)

```csharp
using Confluent.Kafka;

// Same code works with Surgewave or Kafka
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(config).Build();
await producer.ProduceAsync("orders", new Message<string, string> { Value = "order" });
```

## API Categories

### [Producer API](producer.md)
- Single message produce
- Batch produce
- Streaming produce
- Partitioning strategies

### [Consumer API](consumer.md)
- Subscribe/consume
- Manual offset control
- Consumer groups
- Rebalancing

### [Admin Operations](admin.md)
- Topic management
- Cluster operations
- ACL management
- Configuration

## Migration from Kafka

See the [Migration Guide](../migration/index.md) for detailed migration paths:

| Path | Effort | Performance |
|------|--------|-------------|
| Direct Compatibility | Zero | Kafka baseline |
| API Wrapper | Minimal | much lower latency |
| Native Client | Full rewrite | Optimal |

## Next Steps

- [Migration Guide](../migration/index.md) - Migrate from Apache Kafka
- [.NET Client](dotnet.md) - Native client details
- [Confluent.Kafka Wrapper](confluent-kafka-wrapper.md) - Zero-code migration
- [Producer API](producer.md) - Produce messages
- [Consumer API](consumer.md) - Consume messages

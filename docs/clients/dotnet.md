# .NET Client

The Surgewave native .NET client provides high-performance messaging.

## Installation

```bash
dotnet add package Kuestenlogik.Surgewave.Client
```

## Quick Start

```csharp
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Native;

// Create native client
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

// Produce
await client.Messaging.Send("my-topic")
    .WithValue("Hello, Surgewave!")
    .ExecuteAsync();

// Or use the fluent builder
await using var surgewaveClient = await SurgewaveClient.Create("localhost:9092")
    .WithClientId("my-app")
    .BuildAsync();
```

## Typed Producer/Consumer

```csharp
// Typed producer
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.KeySerializer = Serializers.String;
    options.ValueSerializer = Serializers.Json<Order>();
});

await producer.ProduceAsync("orders", "order-123", new Order { Id = 123 });

// Typed consumer
await using var consumer = new SurgewaveConsumer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "order-processor";
    options.ValueDeserializer = Serializers.JsonDeserializer<Order>();
});

consumer.Subscribe("orders");
while (true)
{
    var record = await consumer.ConsumeAsync();
    if (record != null)
        Console.WriteLine($"Order: {record.Value.Id}");
}
```

## Serializers

Built-in serializers:

| Serializer | Type | Description |
|------------|------|-------------|
| `Serializers.String` | string | UTF-8 encoding |
| `Serializers.ByteArray` | byte[] | Pass-through |
| `Serializers.Int32` | int | Big-endian |
| `Serializers.Int64` | long | Big-endian |
| `Serializers.Guid` | Guid | 16-byte binary |
| `Serializers.Json<T>()` | T | System.Text.Json |
| `Serializers.JsonDeserializer<T>()` | T | System.Text.Json (deserializer) |

## Fluent API

```csharp
await client.Messaging.Send("orders")
    .ToPartition(Partitioner.ByKey)
    .WithKey("order-123")
    .WithValue(orderData)
    .WithHeader("correlation-id", correlationId)
    .WithHeader("source", "order-service")
    .UsePreset(SendPreset.LowLatency)
    .ExecuteAsync();
```

## Partitioner Strategies

```csharp
// Round-robin (default)
.ToPartition(Partitioner.RoundRobin)

// By key hash
.ToPartition(Partitioner.ByKey)

// Sticky (same partition until batch)
.ToPartition(Partitioner.Sticky)

// Random
.ToPartition(Partitioner.Random)

// Custom
.ToPartition(new CustomPartitioner())
```

## Send Presets

| Preset | Description |
|--------|-------------|
| `LowLatency` | Immediate send, no batching |
| `Balanced` | Moderate batching (default) |
| `HighThroughput` | Large batches, high linger |
| `Reliable` | Wait for all acks |

## Client Construction

Use the fluent builder or create instances directly:

```csharp
// Fluent builder (auto-detects protocol)
await using var client = await SurgewaveClient.Create("localhost:9092")
    .WithClientId("my-app")
    .UseSurgewaveProtocol()           // or .UseKafkaProtocol()
    .WithTransport(SurgewaveTransportType.Auto)
    .BuildAsync();

// Direct native client construction
await using var native = new SurgewaveNativeClient("localhost", 9092);
await native.ConnectAsync();
```

## Error Handling

```csharp
try
{
    await client.Messaging.Send("topic").WithValue(data).ExecuteAsync();
}
catch (ProtocolException ex)
{
    // Protocol-level error from broker
    Console.WriteLine($"Error: {ex.ErrorCode}");
}
catch (BrokerConnectionException ex)
{
    // Connection failed
    Console.WriteLine($"Connection lost: {ex.Message}");
}
```

## Reactive Extensions

```csharp
// IObservable support
var observable = client.Messaging.AsObservable("events");
observable
    .Where(m => m.Key.StartsWith("important"))
    .Subscribe(m => Process(m));
```

## Admin Operations

Admin operations are available on the `SurgewaveNativeClient`:

```csharp
// Topic management
await client.Topics.CreateAsync("new-topic", partitions: 3);
var topics = await client.Topics.ListAsync();

// Cluster info
var cluster = await client.Cluster.GetClusterInfoAsync();

// Quota management
var quotas = await client.Admin.GetQuotaConfigAsync();
```

## Debugging with Source Link

All Surgewave NuGet packages include Source Link support and publish debug symbols (`.snupkg`) to the NuGet symbol server. This allows you to step into Surgewave source code while debugging.

### Setup (one-time)

In Visual Studio: **Debug > Options > Symbols** and add:
```
https://symbols.nuget.org/download/symbols
```

Also enable: **Debug > Options > General > Enable Source Link support**

In Rider: **Settings > Build > Debugger > Symbol Servers** and add the same URL.

### Verifying

Each assembly embeds the Git commit SHA in its `ProductVersion`:
```csharp
var version = typeof(SurgewaveClient).Assembly
    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
    .InformationalVersion;
// → "0.1.0+9a18fbcb8e4c00295e5d2c37d9d3a8848f046afb"
```

## Next Steps

- [Producer API](producer.md) - Detailed producer guide
- [Consumer API](consumer.md) - Consumer patterns
- [Admin Operations](admin.md) - Administrative APIs

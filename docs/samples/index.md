# Samples

Code examples for Surgewave.

## .NET Examples

### Simple Producer

```csharp
using Kuestenlogik.Surgewave.Client.Native;

await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

// Single message
await client.Messaging.Send("orders")
    .WithKey("order-123")
    .WithValue("New order")
    .ExecuteAsync();

// Batch
await client.Messaging.Send("orders")
    .WithKey("order-1").WithValue(data1)
    .And("order-2", data2)
    .SendAllAsync();
```

### Simple Consumer

```csharp
await using var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
});
consumer.Subscribe("orders");
while (true)
{
    var message = await consumer.ConsumeAsync();
    if (message != null)
        Console.WriteLine($"Key: {message.Key}, Value: {message.Value}");
}
```

### Typed Producer/Consumer

```csharp
// Producer
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.ValueSerializer = Serializers.Json<Order>();
});

await producer.ProduceAsync("orders", "order-123", new Order
{
    Id = 123,
    Customer = "Alice",
    Total = 99.99m
});

// Consumer
await using var consumer = new SurgewaveConsumer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "order-processor";
});

consumer.Subscribe("orders");
while (true)
{
    var record = await consumer.ConsumeAsync();
    if (record != null)
        Console.WriteLine($"Order {record.Value.Id}: ${record.Value.Total}");
}
```

### Confluent.Kafka Compatible

```csharp
using Confluent.Kafka;

// Same code works with Surgewave
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(config).Build();

await producer.ProduceAsync("my-topic", new Message<string, string>
{
    Key = "key",
    Value = "value"
});
```

## Python Examples (gRPC)

### Producer

```python
import grpc
import surgewave_pb2
import surgewave_pb2_grpc

channel = grpc.insecure_channel('localhost:9093')
stub = surgewave_pb2_grpc.ProducerServiceStub(channel)

response = stub.Produce(surgewave_pb2.ProduceRequest(
    topic='my-topic',
    key=b'key',
    value=b'value'
))
print(f'Offset: {response.offset}')
```

### Consumer

```python
stub = surgewave_pb2_grpc.ConsumerServiceStub(channel)

for message in stub.Consume(surgewave_pb2.ConsumeRequest(
    topic='my-topic',
    partition=0,
    offset=0
)):
    print(f'{message.key}: {message.value}')
```

## Common Patterns

### Fire and Forget

```csharp
// No await, returns immediately
_ = client.Messaging.Send("events")
    .WithValue(eventData)
    .ExecuteAsync();
```

### Request-Reply

```csharp
var correlationId = Guid.NewGuid().ToString();

// Send request
await client.Messaging.Send("requests")
    .WithKey(correlationId)
    .WithValue(request)
    .ExecuteAsync();

// Wait for reply using consumer
var consumer = new SurgewaveConsumer<string, byte[]>(opts =>
{
    opts.BootstrapServers = "localhost:9092";
});
consumer.Subscribe("replies");
while (true)
{
    var msg = await consumer.ConsumeAsync();
    if (msg?.Key == correlationId)
        return msg.Value;
}
```

### Fan-Out

```csharp
var topics = new[] { "service-a", "service-b", "service-c" };

await Task.WhenAll(topics.Select(topic =>
    client.Messaging.Send(topic)
        .WithValue(eventData)
        .ExecuteAsync()));
```

## Integration Testing

```csharp
public class OrderTests : IAsyncLifetime
{
    private EmbeddedSurgewave _broker;

    public async Task InitializeAsync()
    {
        _broker = new EmbeddedSurgewave(o => o.Storage = StorageBackend.Memory);
        await _broker.StartAsync();
    }

    public async Task DisposeAsync() => await _broker.DisposeAsync();

    [Fact]
    public async Task OrderCreated_PublishesEvent()
    {
        await using var client = new SurgewaveNativeClient("localhost", 9092);
        await client.ConnectAsync();

        await client.Messaging.Send("orders")
            .WithValue("test")
            .ExecuteAsync();

        // Assert message received
    }
}
```

## Proto Files

gRPC proto files at:
```
src/Kuestenlogik.Surgewave.Grpc/Protos/
├── surgewave.proto
├── producer.proto
├── consumer.proto
└── admin.proto
```

## Sample Applications

Complete sample applications in the `samples/` directory:

### FleetTracker

Real-time fleet tracking dashboard with 20 simulated vehicles.

```
samples/FleetTracker/
├── FleetTracker.Generator/   # Vehicle position simulator
├── FleetTracker.Dashboard/   # Blazor dashboard with map
└── FleetTracker.Shared/      # Shared models
```

**Features:**
- Live map with vehicle markers (Leaflet)
- Vehicle status tracking (Moving/Stopped/Idling)
- Real-time position updates at 1 Hz

**Run:**
```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker
dotnet run --project samples/FleetTracker/FleetTracker.Generator
dotnet run --project samples/FleetTracker/FleetTracker.Dashboard
# Open http://localhost:5000
```

### MassFleetTracker

High-throughput stress test with 100,000 simulated vehicles.

```
samples/MassFleetTracker/
├── MassFleetTracker.Generator/   # 100k vehicle simulator
├── MassFleetTracker.Dashboard/   # Heatmap visualization
└── MassFleetTracker.Shared/      # Shared models
```

**Features:**
- 100,000 vehicles sending position updates
- 100,000 msg/s sustained throughput
- 100 partitions for parallel processing
- 3-tier visualization:
  - **Low zoom:** Heatmap (vehicle density)
  - **Medium zoom:** Cluster markers with counts
  - **High zoom:** Individual vehicle markers

**Data Volume:**

| Metric | Value |
|--------|-------|
| Messages/second | 100,000 |
| Message size | ~150 Bytes (JSON) |
| Throughput | ~15 MB/s |
| Data/hour | ~54 GB |

**Optimization Strategies:**

| Strategy | Data reduction |
|----------|---------------|
| LZ4 compression | 50-70% |
| Binary format (Protobuf) | 70% |
| Reduced frequency (5s) | 80% |
| Delta encoding | 60-80% |

**Run:**
```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker
dotnet run --project samples/MassFleetTracker/MassFleetTracker.Generator
dotnet run --project samples/MassFleetTracker/MassFleetTracker.Dashboard
# Open http://localhost:5000
```

## Next Steps

- [Clients](../clients/index.md) - Full client documentation
- [Quickstart](../quickstart/index.md) - Getting started

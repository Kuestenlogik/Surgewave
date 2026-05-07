# Surgewave Code Snippets

Code snippets demonstrating Surgewave usage patterns.

## C# Snippets

| Snippet | Description |
|---------|-------------|
| [`csharp/SimpleProducer.cs`](csharp/SimpleProducer.cs) | Basic message production |
| [`csharp/SimpleConsumer.cs`](csharp/SimpleConsumer.cs) | Basic message consumption |
| [`csharp/HighThroughputProducer.cs`](csharp/HighThroughputProducer.cs) | High-performance batching producer |
| [`csharp/GrpcProducerExample.cs`](csharp/GrpcProducerExample.cs) | gRPC API producer |
| [`csharp/GrpcConsumerExample.cs`](csharp/GrpcConsumerExample.cs) | gRPC API consumer |

## Python Snippets

See [`python/README.md`](python/README.md) for Python gRPC setup instructions.

| Snippet | Description |
|---------|-------------|
| [`python/producer.py`](python/producer.py) | Python producer using gRPC |
| [`python/consumer.py`](python/consumer.py) | Python consumer using gRPC |

## Prerequisites

Start the Surgewave broker:
```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker
```

## Quick Start

### Producer (C#)

```csharp
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(config).Build();

await producer.ProduceAsync("my-topic", new Message<string, string>
{
    Key = "key",
    Value = "Hello, Surgewave!"
});
```

### Consumer (C#)

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("my-topic");

while (true)
{
    var result = consumer.Consume();
    Console.WriteLine($"Received: {result.Message.Value}");
}
```

## Using Surgewave Native Client

For maximum performance, use the Surgewave Native client:

```csharp
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

// Produce
await using var producer = new SurgewaveBatchingProducer(client, "topic", 0);
await producer.ProduceAsync(null, Encoding.UTF8.GetBytes("message"));
await producer.FlushAsync();

// Consume
var result = await client.Messaging.ReceiveAsync("topic", 0, 0, 64 * 1024);
foreach (var msg in result.Messages)
{
    Console.WriteLine(Encoding.UTF8.GetString(msg.Value.Span));
}
```

## See Also

- [Samples](../../samples/) - Full runnable demo applications
- [Tutorials](../tutorials/) - Step-by-step learning guides

# Tutorial 02: Producers & Consumers

Build a real producer and consumer application with JSON serialization, batching, and error handling.

## Prerequisites

- Completed [Tutorial 01](01-getting-started.md) or have a Surgewave broker running on `localhost:9092`
- .NET 10 SDK installed

## What You Will Build

An order processing system where:
- A **producer** generates order events as JSON
- A **consumer** processes orders and prints summaries

## Step 1: Create the Project

```bash
mkdir surgewave-orders && cd surgewave-orders
dotnet new console -n OrderProducer
dotnet new console -n OrderConsumer
dotnet new classlib -n OrderContracts

# Add references
cd OrderProducer
dotnet add package Kuestenlogik.Surgewave.Client
dotnet add reference ../OrderContracts/OrderContracts.csproj
cd ../OrderConsumer
dotnet add package Kuestenlogik.Surgewave.Client
dotnet add reference ../OrderContracts/OrderContracts.csproj
cd ..
```

## Step 2: Define the Contract

In `OrderContracts/Order.cs`:

```csharp
namespace OrderContracts;

public record Order(
    string OrderId,
    string CustomerId,
    string Product,
    decimal Amount,
    DateTimeOffset CreatedAt);
```

## Step 3: Build the Producer

Replace `OrderProducer/Program.cs`:

```csharp
using Kuestenlogik.Surgewave.Client;
using OrderContracts;

// Create a typed producer -- complex types auto-serialize to JSON
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.BatchSize = 100;
    options.LingerMs = 5;
});

var random = new Random();
var products = new[] { "Widget", "Gadget", "Doohickey", "Thingamajig" };

Console.WriteLine("Producing orders... Press Ctrl+C to stop.");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var count = 0;

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var order = new Order(
            OrderId: $"ORD-{++count:D6}",
            CustomerId: $"CUST-{random.Next(1, 100):D3}",
            Product: products[random.Next(products.Length)],
            Amount: Math.Round((decimal)(random.NextDouble() * 500 + 10), 2),
            CreatedAt: DateTimeOffset.UtcNow);

        var offset = await producer.ProduceAsync(
            "orders", order.CustomerId, order);

        Console.WriteLine(
            $"Produced: {order.OrderId} | {order.Product} | ${order.Amount} -> offset {offset}");

        await Task.Delay(500, cts.Token);
    }
}
catch (OperationCanceledException)
{
    // Normal shutdown
}

await producer.FlushAsync();
Console.WriteLine($"Produced {count} orders total.");
```

## Step 4: Build the Consumer

Replace `OrderConsumer/Program.cs`:

```csharp
using Kuestenlogik.Surgewave.Client;
using OrderContracts;

await using var consumer = new SurgewaveConsumer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "order-processors";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
    options.EnableAutoCommit = false; // Manual commit for reliability
});

consumer.Subscribe("orders");

Console.WriteLine("Consuming orders... Press Ctrl+C to stop.");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var totalAmount = 0m;
var processed = 0;

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var result = await consumer.ConsumeAsync(cts.Token);
        if (result is null)
            continue;

        var order = result.Value;
        totalAmount += order.Amount;
        processed++;

        Console.WriteLine(
            $"[Partition {result.Partition}, Offset {result.Offset}] " +
            $"{order.OrderId}: {order.Product} ${order.Amount} " +
            $"(Customer: {order.CustomerId})");

        // Commit after successful processing
        await consumer.CommitAsync(
            result.Topic, result.Partition, result.Offset + 1);
    }
}
catch (OperationCanceledException)
{
    // Normal shutdown
}

Console.WriteLine($"Processed {processed} orders, total: ${totalAmount:F2}");
```

## Step 5: Run It

Open two terminals.

**Terminal 1 -- start the consumer first:**

```bash
cd OrderConsumer
dotnet run
```

**Terminal 2 -- start the producer:**

```bash
cd OrderProducer
dotnet run
```

You should see orders flowing from producer to consumer in real time.

## Batch Production

For high-throughput scenarios, use the native client's batch API:

```csharp
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

// Send multiple messages in a single call
await client.Messaging.Send("orders")
    .WithKey("CUST-001").WithValue(orderJson1)
    .And("CUST-002", orderJson2)
    .And("CUST-003", orderJson3)
    .SendAllAsync();
```

Or use the `HighThroughput` preset for automatic batching and compression:

```csharp
await client.Messaging.Send("orders")
    .UsePreset(SendPreset.HighThroughput)
    .WithKey("CUST-001")
    .WithValue(orderData)
    .ExecuteAsync();
```

## Error Handling

Handle produce errors gracefully:

```csharp
try
{
    await producer.ProduceAsync("orders", key, order);
}
catch (SurgewaveException ex)
{
    switch (ex.ErrorCode)
    {
        case SurgewaveErrorCode.TopicNotFound:
            Console.WriteLine("Topic does not exist. Create it first.");
            break;
        case SurgewaveErrorCode.MessageTooLarge:
            Console.WriteLine("Message exceeds size limit.");
            break;
        default:
            Console.WriteLine($"Produce error: {ex.Message}");
            throw;
    }
}
```

Handle consume errors with reconnection:

```csharp
consumer.Disconnected += (sender, args) =>
{
    Console.WriteLine($"Lost connection: {args.Exception.Message}");
};

consumer.Reconnected += (sender, args) =>
{
    Console.WriteLine("Reconnected to broker.");
};
```

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Key** | Messages with the same key go to the same partition, guaranteeing order |
| **Serialization** | Complex types auto-serialize to JSON via `System.Text.Json` |
| **BatchSize / LingerMs** | Control batching: larger batches = higher throughput, higher latency |
| **Manual Commit** | `EnableAutoCommit = false` lets you commit after processing for at-least-once delivery |
| **FlushAsync** | Ensure all buffered messages are sent before shutting down |

## Next Steps

- [Tutorial 03: Consumer Groups](03-consumer-groups.md) -- scale consumers with load balancing
- [Producer API Reference](../clients/producer.md) -- full producer configuration
- [Consumer API Reference](../clients/consumer.md) -- full consumer configuration

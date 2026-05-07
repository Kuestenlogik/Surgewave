# Tutorial 03: Consumer Groups

Scale message processing with consumer groups, rebalancing, and offset management.

## Prerequisites

- Completed [Tutorial 02](02-producers-consumers.md) or have a Surgewave broker running on `localhost:9092`
- .NET 10 SDK installed

## What You Will Build

A scalable order processing system where multiple consumer instances share the workload. You will:
- Run multiple consumers in the same group
- Observe partition assignment and rebalancing
- Implement manual offset management
- Handle consumer group events

## Step 1: Create the Project

```bash
mkdir surgewave-consumer-groups && cd surgewave-consumer-groups
dotnet new console -n GroupConsumer
cd GroupConsumer
dotnet add package Kuestenlogik.Surgewave.Client
```

## Step 2: Create a Multi-Partition Topic

First, create a topic with multiple partitions using the CLI:

```bash
surgewave topics create order-events --partitions 6
```

Or programmatically:

```csharp
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

await client.Topics.CreateAsync("order-events", partitions: 6);
```

## Step 3: Build the Group Consumer

Replace `GroupConsumer/Program.cs`:

```csharp
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Native;

var consumerId = args.Length > 0 ? args[0] : $"consumer-{Random.Shared.Next(1000)}";

Console.WriteLine($"Starting consumer: {consumerId}");

await using var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "order-processors";    // Same group = shared workload
    options.ClientId = consumerId;
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
    options.EnableAutoCommit = false;
    options.SessionTimeoutMs = 30000;
    options.HeartbeatIntervalMs = 3000;
});

// Handle rebalancing events
consumer.PartitionsAssigned += (sender, args) =>
{
    Console.WriteLine($"[{consumerId}] Partitions ASSIGNED: " +
        string.Join(", ", args.Partitions));
};

consumer.PartitionsRevoked += (sender, args) =>
{
    Console.WriteLine($"[{consumerId}] Partitions REVOKED: " +
        string.Join(", ", args.Partitions));
    // Commit current offsets before losing partitions
    consumer.CommitAsync().Wait();
};

// Subscribe triggers group join
await consumer.SubscribeAsync(CancellationToken.None, "order-events");

Console.WriteLine($"[{consumerId}] Joined group 'order-processors'. Waiting for messages...");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var batchCount = 0;
var batch = new List<(string Topic, int Partition, long Offset)>();

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var result = await consumer.ConsumeAsync(cts.Token);
        if (result is null)
            continue;

        Console.WriteLine(
            $"[{consumerId}] Partition {result.Partition} | " +
            $"Offset {result.Offset} | Key: {result.Key} | Value: {result.Value}");

        batch.Add((result.Topic, result.Partition, result.Offset));
        batchCount++;

        // Commit in batches of 10 for efficiency
        if (batchCount >= 10)
        {
            var last = batch.Last();
            await consumer.CommitAsync(last.Topic, last.Partition, last.Offset + 1);
            Console.WriteLine($"[{consumerId}] Committed offset {last.Offset + 1}");
            batch.Clear();
            batchCount = 0;
        }
    }
}
catch (OperationCanceledException)
{
    // Commit remaining offsets on shutdown
    if (batch.Count > 0)
    {
        var last = batch.Last();
        await consumer.CommitAsync(last.Topic, last.Partition, last.Offset + 1);
    }
}

Console.WriteLine($"[{consumerId}] Shutting down.");
```

## Step 4: Produce Test Data

Create a simple producer to generate messages across all 6 partitions:

```bash
dotnet new console -n GroupProducer
cd GroupProducer
dotnet add package Kuestenlogik.Surgewave.Client
```

`GroupProducer/Program.cs`:

```csharp
using Kuestenlogik.Surgewave.Client;

await using var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
});

for (var i = 1; i <= 60; i++)
{
    var key = $"customer-{i % 10}";
    var value = $"Order #{i} for {key}";

    await producer.ProduceAsync("order-events", key, value);
    Console.WriteLine($"Produced: {key} -> {value}");

    await Task.Delay(200);
}

await producer.FlushAsync();
Console.WriteLine("Done producing.");
```

## Step 5: Run It

Open **three** terminals to see rebalancing in action.

**Terminal 1 -- first consumer:**

```bash
cd GroupConsumer
dotnet run -- consumer-A
```

**Terminal 2 -- second consumer (same group):**

```bash
cd GroupConsumer
dotnet run -- consumer-B
```

**Terminal 3 -- produce data:**

```bash
cd GroupProducer
dotnet run
```

### What You Should Observe

1. When consumer-A starts alone, it gets all 6 partitions.
2. When consumer-B joins, a **rebalance** occurs: each consumer gets 3 partitions.
3. Messages are distributed: each consumer processes only its assigned partitions.
4. If you stop consumer-B (Ctrl+C), consumer-A gets all 6 partitions back.

## Consumer Group Semantics

### Partition Assignment

Surgewave distributes partitions evenly across group members:

```
6 partitions, 1 consumer:  A gets [0,1,2,3,4,5]
6 partitions, 2 consumers: A gets [0,1,2], B gets [3,4,5]
6 partitions, 3 consumers: A gets [0,1], B gets [2,3], C gets [4,5]
6 partitions, 6 consumers: Each gets exactly 1 partition
6 partitions, 7 consumers: One consumer is idle (no partition)
```

### Offset Management Strategies

**Auto-commit** (simplest, at-most-once risk):

```csharp
options.EnableAutoCommit = true;
options.AutoCommitIntervalMs = 5000; // Commit every 5 seconds
```

**Manual commit after each message** (safest, lower throughput):

```csharp
var result = await consumer.ConsumeAsync(ct);
if (result != null)
{
    await ProcessAsync(result);
    await consumer.CommitAsync(result.Topic, result.Partition, result.Offset + 1);
}
```

**Batch commit** (best balance of safety and throughput):

```csharp
var count = 0;
ConsumeResult<string, string>? lastResult = null;

while (!ct.IsCancellationRequested)
{
    var result = await consumer.ConsumeAsync(ct);
    if (result is null) continue;

    await ProcessAsync(result);
    lastResult = result;

    if (++count >= 100)
    {
        await consumer.CommitAsync(
            lastResult.Topic, lastResult.Partition, lastResult.Offset + 1);
        count = 0;
    }
}
```

## Monitoring Consumer Lag

Check how far behind a consumer group is:

```csharp
var allLag = await consumer.GetAllLagAsync(CancellationToken.None);

foreach (var ((topic, partition), lag) in allLag)
{
    Console.WriteLine($"{topic}[{partition}]: {lag} messages behind");
}
```

Or use the CLI:

```bash
surgewave groups describe order-processors
```

## Seek and Replay

Reset a consumer to re-process messages:

```csharp
// Seek to the beginning of a partition
consumer.Seek("order-events", partition: 0, offset: 0);

// Seek to a specific offset
consumer.Seek("order-events", partition: 0, offset: 12345);

// Get current position
long position = consumer.Position("order-events", partition: 0);
```

## Pause and Resume

Temporarily stop consuming from specific partitions:

```csharp
// Pause partitions 0 and 1
consumer.Pause(("order-events", 0), ("order-events", 1));

// Resume later
consumer.Resume(("order-events", 0), ("order-events", 1));
```

## Next Steps

- [Tutorial 04: Kafka Connect](04-kafka-connect.md) -- build connectors for external systems
- [Consumer API Reference](../clients/consumer.md) -- full consumer configuration and patterns
- [Monitoring](../monitoring/dashboard.md) -- track consumer lag in the Control dashboard

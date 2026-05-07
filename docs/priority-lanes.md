# Priority Lanes

Priority Lanes let producers mark messages as `High`, `Normal`, or `Low` priority. The broker
routes each message to a dedicated partition range, and priority consumers drain high-priority
partitions before lower-priority ones, guaranteeing that urgent messages are processed first even
under load.

## Overview

Surgewave implements priority through a convention: a single logical topic is divided into three
physical partition ranges — one per priority level. The `PriorityPartitioner` reads the
`surgewave-priority` header and routes each message into the appropriate range.

```
Topic: "orders"  (PartitionsPerPriority = 2, TotalPartitions = 6)

Partition 0-1  →  High   priority messages
Partition 2-3  →  Normal priority messages (default)
Partition 4-5  →  Low    priority messages
```

## `MessagePriority` Enum

```csharp
public enum MessagePriority
{
    High   = 0,   // routed to partitions 0 … P-1
    Normal = 1,   // routed to partitions P … 2P-1 (default when header absent)
    Low    = 2    // routed to partitions 2P … 3P-1
}
```

## The `surgewave-priority` Header

Priority is conveyed as a UTF-8 string in the `surgewave-priority` message header.

| Header Value | Priority Level |
|--------------|---------------|
| `"high"` | `MessagePriority.High` |
| `"normal"` | `MessagePriority.Normal` |
| `"low"` | `MessagePriority.Low` |
| *(absent)* | `MessagePriority.Normal` |

The value is case-insensitive. Any unrecognised value falls back to `Normal`.

### Setting the header manually

```csharp
var headers = new Dictionary<string, byte[]>()
    .WithPriority(MessagePriority.High);

await producer.ProduceAsync("orders", key, value, headers);
```

## `PriorityPartitioner`

`PriorityPartitioner` implements `IPartitionStrategy` and reads the `surgewave-priority` header to
select the correct partition range. Within a range, it delegates to an inner strategy (default:
round-robin) to spread load evenly.

### Configuration

```csharp
var partitioner = new PriorityPartitioner(new PriorityPartitionerOptions
{
    PartitionsPerPriority = 2,               // 2 partitions per level = 6 total
    InnerStrategy = Partitioner.RoundRobin   // default
});
```

### Using with a producer

```csharp
await using var producer = new SurgewaveProducer<string, Order>(opts =>
{
    opts.BootstrapServers = "localhost:9092";
    opts.PartitionStrategy = new PriorityPartitioner(new PriorityPartitionerOptions
    {
        PartitionsPerPriority = 2
    });
});

// High-priority order — routed to partition 0 or 1
var highPriorityHeaders = new Dictionary<string, byte[]>()
    .WithPriority(MessagePriority.High);

await producer.ProduceAsync("orders", "ord-001", urgentOrder, highPriorityHeaders);

// Normal priority (no header needed — Normal is the default)
await producer.ProduceAsync("orders", "ord-002", regularOrder);
```

## Partition Layout

With `PartitionsPerPriority = P` the topic must have exactly `3 × P` partitions:

```
Index   0 … P-1          → High
Index   P … 2P-1         → Normal
Index   2P … 3P-1        → Low
```

Create the topic with the correct partition count before starting producers:

```bash
surgewave-cli topics create orders --partitions 6
```

## `PriorityConsumerConfig`

On the consumer side, `PriorityConsumerConfig` controls how poll budget is divided across
priority levels.

```csharp
public sealed class PriorityConsumerConfig
{
    public int HighWeight   { get; init; } = 3;  // default poll ratio
    public int NormalWeight { get; init; } = 2;
    public int LowWeight    { get; init; } = 1;

    public int PartitionsPerPriority { get; init; } = 1;
    public bool DrainHighBeforeLow   { get; init; } = true;
}
```

### Weighted polling

Each poll cycle has a budget of `HighWeight + NormalWeight + LowWeight` tokens. With the default
weights (3:2:1) the consumer polls high-priority partitions three times more often than
low-priority ones.

When `DrainHighBeforeLow = true` (the default), the consumer exhausts all available messages in
high-priority partitions before processing any normal or low-priority messages within the same
budget cycle.

### Poll schedule

`BuildPollSchedule()` returns the ordered priority sequence for a single budget cycle:

```
Default 3:2:1 schedule:
  High, High, High, Normal, Normal, Low
```

### Consumer setup

```csharp
var priorityConfig = new PriorityConsumerConfig
{
    HighWeight = 5,
    NormalWeight = 2,
    LowWeight = 1,
    PartitionsPerPriority = 2,
    DrainHighBeforeLow = true
};

// Manually assign all partitions for the priority consumer
consumer.Assign("orders", partition: 0, offset: -1);  // High-0
consumer.Assign("orders", partition: 1, offset: -1);  // High-1
consumer.Assign("orders", partition: 2, offset: -1);  // Normal-0
consumer.Assign("orders", partition: 3, offset: -1);  // Normal-1
consumer.Assign("orders", partition: 4, offset: -1);  // Low-0
consumer.Assign("orders", partition: 5, offset: -1);  // Low-1

foreach (var priority in priorityConfig.BuildPollSchedule())
{
    foreach (var partition in priorityConfig.GetPartitionsForPriority(priority))
    {
        var result = await consumer.ConsumeAsync(cancellationToken);
        if (result != null)
            await ProcessAsync(result, priority);
    }
}
```

## Complete End-to-End Example

```csharp
// Producer
var partitioner = new PriorityPartitioner(new PriorityPartitionerOptions
{
    PartitionsPerPriority = 1  // 3 total partitions: 0=High, 1=Normal, 2=Low
});

await using var producer = new SurgewaveProducer<string, string>(opts =>
{
    opts.BootstrapServers = "localhost:9092";
    opts.PartitionStrategy = partitioner;
});

// Mark as high priority
await producer.ProduceAsync(
    "alerts",
    key: "critical",
    value: "disk full",
    headers: new Dictionary<string, byte[]>().WithPriority(MessagePriority.High));

// Normal priority (default — no header required)
await producer.ProduceAsync("alerts", key: "info", value: "backup complete");

// Mark as low priority
await producer.ProduceAsync(
    "alerts",
    key: "debug",
    value: "cache eviction stats",
    headers: new Dictionary<string, byte[]>().WithPriority(MessagePriority.Low));
```

## Limitations

- The topic must be created with exactly `3 × PartitionsPerPriority` partitions before use.
  The partitioner does not create or resize the topic automatically.
- Consumer group rebalancing assigns partitions randomly unless you use manual partition
  assignment. For strict priority ordering, assign partitions manually as shown above.
- Priority is a best-effort guarantee under load, not a hard real-time constraint. A fully
  saturated consumer may still process some lower-priority messages while draining high.

## Next Steps

- [Producer API](clients/producer.md) — Producing messages with headers
- [Consumer API](clients/consumer.md) — Poll-based consumption
- [Streaming Consumer](streaming-consumer.md) — Push-based low-latency consumption

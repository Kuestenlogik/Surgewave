# Consumer API

Complete guide to consuming messages with Surgewave.

## Quick Start

The recommended way to consume messages is using the typed `SurgewaveConsumer<TKey, TValue>`:

```csharp
await using var consumer = new SurgewaveConsumer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "order-processor";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
});

consumer.Subscribe("orders");

while (!cancellationToken.IsCancellationRequested)
{
    var result = await consumer.ConsumeAsync(cancellationToken);
    if (result != null)
    {
        Console.WriteLine($"Order {result.Value.Id}: {result.Value.Status}");
        await consumer.CommitAsync(result.Topic, result.Partition, result.Offset + 1);
    }
}
```

## SurgewaveConsumer Configuration

### Basic Setup

```csharp
await using var consumer = new SurgewaveConsumer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "my-group";
    options.ClientId = "my-consumer";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
    options.EnableAutoCommit = true;
    options.AutoCommitIntervalMs = 5000;
});
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `BootstrapServers` | string | Required | Broker address (host:port) |
| `GroupId` | string | null | Consumer group ID (optional for simple consumers) |
| `ClientId` | string | null | Client identifier for logging |
| `KeyDeserializer` | IDeserializer | Auto | Key deserializer (auto-detected by type) |
| `ValueDeserializer` | IDeserializer | Auto | Value deserializer (auto-detected by type) |
| `AutoOffsetReset` | AutoOffsetReset | Latest | Where to start if no offset stored |
| `EnableAutoCommit` | bool | true | Auto-commit offsets |
| `AutoCommitIntervalMs` | int | 5000 | Auto-commit interval |
| `SessionTimeoutMs` | int | 30000 | Session timeout for consumer groups |
| `HeartbeatIntervalMs` | int | 3000 | Heartbeat interval for consumer groups |
| `MaxPollIntervalMs` | int | 300000 | Max time between polls |
| `FetchMaxBytes` | int | 1MB | Maximum bytes per fetch |
| `IsolationLevel` | IsolationLevel | ReadUncommitted | Transaction isolation |
| `Transport` | SurgewaveTransportType | Auto | Transport type |

### Auto-Reconnect Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `EnableAutoReconnect` | bool | true | Auto-reconnect on connection loss |
| `MaxReconnectAttempts` | int | 10 | Max reconnection attempts |
| `ReconnectBackoffMs` | int | 100 | Initial backoff between retries |
| `ReconnectBackoffMaxMs` | int | 10000 | Maximum backoff (10 seconds) |

## Subscribing to Topics

### Simple Subscribe

```csharp
// Subscribe to single topic (partition discovery happens lazily on first consume)
consumer.Subscribe("orders");

// Subscribe to multiple topics
consumer.Subscribe("orders", "payments", "inventory");
```

### Async Subscribe with Partition Discovery

```csharp
// Discovers all partitions and joins consumer group if GroupId is set
await consumer.SubscribeAsync(cancellationToken, "orders", "payments");
```

### Manual Partition Assignment

```csharp
// Assign specific partition with starting offset
consumer.Assign("orders", partition: 0, offset: 0);
consumer.Assign("orders", partition: 1, offset: 100);
```

## Consuming Messages

### Single Message

```csharp
var result = await consumer.ConsumeAsync(cancellationToken);
if (result != null)
{
    Console.WriteLine($"Topic: {result.Topic}, Partition: {result.Partition}");
    Console.WriteLine($"Key: {result.Key}, Value: {result.Value}");
    Console.WriteLine($"Offset: {result.Offset}, Timestamp: {result.Timestamp}");
}
```

### With Timeout

```csharp
var result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(5), cancellationToken);
```

### Continuous Consumption Loop

```csharp
while (!cancellationToken.IsCancellationRequested)
{
    var result = await consumer.ConsumeAsync(cancellationToken);
    if (result != null)
    {
        await ProcessAsync(result);
    }
}
```

## Handler-Based Dispatch

For polymorphic messages, register type-specific handlers:

```csharp
await using var consumer = new SurgewaveConsumer<string, OrderEvent>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.ValueDeserializer = Serializers.PolymorphicJsonDeserializer<OrderEvent>(
        typeof(OrderCreated), typeof(OrderShipped), typeof(OrderDelivered));
});

// Register handlers for each event type
consumer
    .OnMessage<OrderCreated>(async (msg, ct) =>
    {
        Console.WriteLine($"Order created: {msg.Value.OrderId}");
    })
    .OnMessage<OrderShipped>(async (msg, ct) =>
    {
        Console.WriteLine($"Order shipped: {msg.Value.TrackingNumber}");
    })
    .OnMessage<OrderDelivered>(async (msg, ct) =>
    {
        Console.WriteLine($"Order delivered: {msg.Value.DeliveredAt}");
    });

// Default handler for unmatched types
consumer.OnMessage(async (msg, ct) =>
{
    Console.WriteLine($"Unknown event type: {msg.Value?.GetType().Name}");
});

consumer.Subscribe("order-events");

// Run the dispatch loop
await consumer.ConsumeLoopAsync(cancellationToken);
```

### Single Message Dispatch

```csharp
// Consume and dispatch single message
bool processed = await consumer.ConsumeAndDispatchAsync(cancellationToken);
```

## Consumer Groups

### Joining a Group

```csharp
var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "my-consumer-group";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
    options.EnableAutoCommit = true;
});

// SubscribeAsync joins the consumer group
await consumer.SubscribeAsync(cancellationToken, "topic1", "topic2");
```

### Rebalancing Events

Handle partition assignment changes using events:

```csharp
consumer.PartitionsAssigned += (sender, args) =>
{
    Console.WriteLine($"Assigned: {string.Join(", ", args.Partitions)}");
};

consumer.PartitionsRevoked += (sender, args) =>
{
    Console.WriteLine($"Revoked: {string.Join(", ", args.Partitions)}");
    // Commit offsets before losing partitions
    consumer.CommitAsync().Wait();
};
```

### Connection Events

```csharp
consumer.Disconnected += (sender, args) =>
{
    Console.WriteLine($"Disconnected: {args.Exception.Message}");
};

consumer.Reconnected += (sender, args) =>
{
    Console.WriteLine("Reconnected to broker");
};
```

## Offset Management

### Manual Commit

```csharp
var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.GroupId = "precise-processor";
    options.EnableAutoCommit = false;  // Disable auto-commit
});

await consumer.SubscribeAsync(cancellationToken, "important-events");

while (!cancellationToken.IsCancellationRequested)
{
    var result = await consumer.ConsumeAsync(cancellationToken);
    if (result != null)
    {
        try
        {
            await ProcessAsync(result);
            // Commit after successful processing
            await consumer.CommitAsync(result.Topic, result.Partition, result.Offset + 1);
        }
        catch (Exception ex)
        {
            // Don't commit - will retry on restart
            Log.Error($"Failed to process: {ex.Message}");
        }
    }
}
```

### Commit All Partitions

```csharp
// Commit current offsets for all assigned partitions
await consumer.CommitAsync(cancellationToken);
```

### Batch Commit

```csharp
var batch = new List<ConsumeResult<string, Order>>();

while (!cancellationToken.IsCancellationRequested)
{
    var result = await consumer.ConsumeAsync(cancellationToken);
    if (result != null)
    {
        batch.Add(result);

        if (batch.Count >= 100)
        {
            await ProcessBatchAsync(batch);
            var last = batch.Last();
            await consumer.CommitAsync(last.Topic, last.Partition, last.Offset + 1);
            batch.Clear();
        }
    }
}
```

## Seek Operations

Seek to a specific offset:

```csharp
// Seek to specific offset
consumer.Seek("orders", partition: 0, offset: 12345);

// Seek to beginning (offset 0)
consumer.Seek("orders", partition: 0, offset: 0);

// Current position
long position = consumer.Position("orders", partition: 0);
```

## Pause and Resume

```csharp
// Pause consumption from specific partitions
consumer.Pause(("orders", 0), ("orders", 1));

// Check paused partitions
var paused = consumer.PausedPartitions;

// Resume consumption
consumer.Resume(("orders", 0), ("orders", 1));
```

## Lag Monitoring

```csharp
// Get lag for specific partition
long lag = await consumer.GetLagAsync("orders", partition: 0, cancellationToken);

// Get lag for all assigned partitions
var allLag = await consumer.GetAllLagAsync(cancellationToken);
foreach (var ((topic, partition), lagValue) in allLag)
{
    Console.WriteLine($"{topic}[{partition}]: {lagValue} messages behind");
}

// Check current assignment
var assignment = consumer.Assignment;
```

## Consumption Patterns

### At-Most-Once

Commit before processing (fastest, may lose messages):

```csharp
var result = await consumer.ConsumeAsync(cancellationToken);
if (result != null)
{
    await consumer.CommitAsync(result.Topic, result.Partition, result.Offset + 1);
    await ProcessAsync(result);  // May fail after commit
}
```

### At-Least-Once

Commit after processing (safest, may duplicate):

```csharp
var result = await consumer.ConsumeAsync(cancellationToken);
if (result != null)
{
    await ProcessAsync(result);  // Process first
    await consumer.CommitAsync(result.Topic, result.Partition, result.Offset + 1);  // Then commit
}
```

### Exactly-Once

Use transactions for exactly-once semantics (see [Transactions](../features/transactions.md)):

```csharp
var tx = nativeClient.Transactions.BeginTransaction("my-txn-id");
await tx.InitAsync();

var result = await consumer.ConsumeAsync(cancellationToken);
if (result != null)
{
    await ProcessAndProduceAsync(result);
    await tx.CommitAsync();  // Commits atomically
}
```

## Error Handling

```csharp
try
{
    while (!cancellationToken.IsCancellationRequested)
    {
        var result = await consumer.ConsumeAsync(cancellationToken);
        if (result != null)
            await ProcessAsync(result);
    }
}
catch (BrokerConnectionException ex)
{
    // Connection failed after max retries
    Log.Error($"Connection lost: {ex.Message}");
}
catch (InvalidConfigurationException ex)
{
    // Configuration error (no topics, invalid group, etc.)
    Log.Error($"Configuration error: {ex.Message}");
}
catch (ProtocolException ex)
{
    // Protocol-level error
    Log.Error($"Protocol error: {ex.ErrorCode}");
}
```

## Performance Tips

1. **Use handler dispatch** - For polymorphic messages, handlers are more efficient than switch statements
2. **Batch commits** - Don't commit every message; commit in batches for throughput
3. **Increase fetch size** - Larger `FetchMaxBytes` for high-throughput scenarios
4. **Use SharedMemory transport** - For same-machine scenarios, get ultra-low latency (target)
5. **Parallel processing** - Process messages concurrently while respecting partition ordering
6. **Monitor lag** - Use `GetLagAsync()` to detect slow consumers

## Next Steps

- [Producer API](producer.md) - Produce messages
- [Streaming Consumer](../streaming-consumer.md) - Push-based alternative to polling
- [Priority Lanes](../priority-lanes.md) - Route and consume messages by priority
- [Transactions](../features/transactions.md) - Exactly-once semantics
- [Performance](../performance/tuning.md) - Optimization guide

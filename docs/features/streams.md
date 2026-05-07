# Surgewave Streams

Surgewave Streams is a native .NET stream processing library for building real-time data pipelines and applications.

## Overview

The Streams library provides:
- **Stream** - Stateless record stream transformations
- **Table** - Stateful changelog stream (key-value updates)
- **Windows** - Time-based grouping (tumbling, hopping, sliding, session)
- **State Stores** - Persistent and in-memory stores for aggregations

## Current Status

| Component | Status |
|-----------|--------|
| StreamsBuilder | Implemented |
| Stream operations | Implemented |
| Table operations | Implemented |
| Windowing | Implemented |
| State stores | Implemented |
| Joins (stream-stream, stream-table, FK table-table) | Implemented |
| Stream runtime | Implemented |
| Processor lifecycle hooks | Implemented |
| Per-processor metrics (OTEL) | Implemented |
| Backpressure & flow control | Implemented |
| Retry & backoff | Implemented |

## Building a Topology

Use `StreamsBuilder` to define your stream processing topology:

```csharp
var builder = new StreamsBuilder();

// Create a stream from a topic
var stream = builder.Stream<string, Order>("orders");

// Transform and route
stream
    .Filter((key, order) => order.Amount > 100)
    .MapValues(order => new OrderSummary(order.Id, order.Amount))
    .To("large-orders");

// Build the topology
var topology = builder.Build();
```

## Stream Operations

### Filter

Filter records based on a predicate:

```csharp
stream.Filter((key, value) => value.Amount > 100)
stream.FilterNot((key, value) => value.IsCancelled)
```

### Map

Transform keys and/or values:

```csharp
// Map values only
stream.MapValues(order => order.Amount * 1.1m)

// Map values with key access
stream.MapValues((key, order) => $"{key}: {order.Status}")

// Map both key and value
stream.Map((key, value) => KeyValue.Pair(value.CustomerId, value))

// Change key only
stream.SelectKey((key, value) => value.CustomerId)
```

### FlatMap

Produce zero or more output records per input:

```csharp
// Expand order items
stream.FlatMapValues(order => order.Items)

// Full key-value flatmap
stream.FlatMap((key, order) =>
    order.Items.Select(item => KeyValue.Pair(item.ProductId, item)))
```

### Peek

Side effects without modifying the stream:

```csharp
stream.Peek((key, value) => Console.WriteLine($"Processing: {key}"))
```

### Branch

Split stream by predicates:

```csharp
var branches = stream.Branch(
    (k, v) => v.Priority == Priority.High,
    (k, v) => v.Priority == Priority.Medium,
    (k, v) => true  // default branch
);

branches[0].To("high-priority");
branches[1].To("medium-priority");
branches[2].To("low-priority");
```

### Merge

Combine multiple streams:

```csharp
var merged = stream1.Merge(stream2);
```

### Repartition

Repartition stream by a new key:

```csharp
stream.Repartition()  // Keep same key
stream.Repartition<string>((key, value) => value.Region)  // New key
```

## Table Operations

Create a table from a changelog topic:

```csharp
var users = builder.Table<string, User>("users");

// Filter active users
var activeUsers = users.Filter((key, user) => user.IsActive);

// Transform values
var userNames = users.MapValues(user => user.FullName);

// Convert to stream
userNames.ToStream().To("user-names");
```

## Grouping and Aggregation

### Group By Key

```csharp
var grouped = stream.GroupByKey();
```

### Group By Custom Key

```csharp
var groupedByCategory = stream.GroupBy<string>((key, order) => order.Category);
```

### Count

```csharp
var counts = stream
    .GroupByKey()
    .Count();
```

### Reduce

```csharp
var totals = stream
    .GroupByKey()
    .Reduce((agg, value) => agg + value.Amount);
```

### Aggregate

```csharp
var stats = stream
    .GroupByKey()
    .Aggregate(
        initializer: () => new OrderStats(),
        aggregator: (key, order, stats) => stats.Add(order),
        materialized: Materialized.As<string, OrderStats>("order-stats"));
```

## Windowing

### Tumbling Windows

Fixed-size, non-overlapping windows:

```csharp
stream
    .GroupByKey()
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(5)))
    .Count();
```

### Hopping Windows

Fixed-size, overlapping windows:

```csharp
stream
    .GroupByKey()
    .WindowedBy(HoppingWindows.Of(TimeSpan.FromMinutes(5))
        .AdvanceBy(TimeSpan.FromMinutes(1)))
    .Count();
```

### Sliding Windows

Time-difference based windows:

```csharp
stream
    .GroupByKey()
    .WindowedBy(SlidingWindows.Of(TimeSpan.FromMinutes(5)))
    .Count();
```

### Session Windows

Inactivity-gap based windows:

```csharp
stream
    .GroupByKey()
    .WindowedBy(SessionWindows.With(TimeSpan.FromMinutes(30)))
    .Count();
```

## Joins

### Stream-Stream Join

Join two streams within a time window:

```csharp
var orders = builder.Stream<string, Order>("orders");
var payments = builder.Stream<string, Payment>("payments");

var joined = orders.Join(
    payments,
    (order, payment) => new OrderPayment(order, payment),
    JoinWindows.Of(TimeSpan.FromHours(1)));
```

### Stream-Table Join

Enrich stream with table lookup:

```csharp
var orders = builder.Stream<string, Order>("orders");
var customers = builder.Table<string, Customer>("customers");

var enriched = orders.Join(
    customers,
    (order, customer) => new EnrichedOrder(order, customer));
```

### Left Join

Include records even when join key not found:

```csharp
var enriched = orders.LeftJoin(
    customers,
    (order, customer) => new EnrichedOrder(order, customer ?? Customer.Unknown));
```

## State Stores

### In-Memory Key-Value Store

```csharp
var store = Stores.InMemoryKeyValueStore<string, Order>("orders-store");
builder.AddStateStore(store);
```

### Persistent Key-Value Store

```csharp
var store = Stores.PersistentKeyValueStore<string, Order>("orders-store");
builder.AddStateStore(store);
```

### Window Store

```csharp
var store = Stores.PersistentWindowStore<string, long>(
    name: "counts-store",
    retention: TimeSpan.FromDays(1),
    windowSize: TimeSpan.FromMinutes(5));
```

### Session Store

```csharp
var store = Stores.PersistentSessionStore<string, long>(
    name: "session-store",
    retention: TimeSpan.FromDays(1));
```

## Serialization (Serdes)

Built-in serializers/deserializers:

```csharp
Serdes.String()      // string
Serdes.Int()         // int
Serdes.Long()        // long
Serdes.ByteArray()   // byte[]
Serdes.Json<T>()     // JSON serialization
```

## Configuration

```csharp
var config = new StreamsConfig
{
    ApplicationId = "my-streams-app",
    BootstrapServers = "localhost:9092",
    DefaultKeySerde = Serdes.String(),
    DefaultValueSerde = Serdes.String(),
    StateDir = "/var/surgewave/streams",
    CommitIntervalMs = 1000,
    CacheMaxBytesBuffering = 10 * 1024 * 1024
};
```

## Foreign Key Table-Table Joins

Join two tables where the primary table references the foreign table via a key extractor function. Unlike standard joins that require matching keys, FK joins let you join on any extracted key.

### Inner Join

```csharp
var orders = builder.Table<string, Order>("orders");
var customers = builder.Table<string, Customer>("customers");

// Join orders to customers via the CustomerId field
var enriched = orders.Join<string, Customer, EnrichedOrder>(
    customers,
    order => order.CustomerId,  // FK extractor
    (order, customer) => new EnrichedOrder(order, customer));
```

### Left Join

Include primary records even when no foreign match exists:

```csharp
var enriched = orders.LeftJoin<string, Customer, EnrichedOrder>(
    customers,
    order => order.CustomerId,
    (order, customer) => new EnrichedOrder(order, customer ?? Customer.Unknown));
```

Both sides are backed by automatically managed state stores. When either side updates, affected join results are re-emitted.

## Processor Lifecycle Hooks

Custom processors can implement `IProcessorLifecycle` to receive initialization and shutdown callbacks:

```csharp
public interface IProcessorLifecycle
{
    void OnInit(ProcessorLifecycleContext context);
    void OnClose(ProcessorLifecycleContext context);
}
```

The `ProcessorLifecycleContext` provides:

| Property | Type | Description |
|----------|------|-------------|
| `NodeName` | string | The processor node name |
| `ProcessorContext` | ProcessorContext | Access to state stores and forwarding |
| `ShutdownToken` | CancellationToken | Cancelled when shutdown is requested |
| `ShutdownTimeout` | TimeSpan | Maximum time allowed for cleanup |

### Graceful Shutdown

`ShutdownOrchestrator` closes nodes in reverse-topological order (sinks first, then processors, then sources). Each node gets a per-node timeout of 10 seconds for its `OnClose` callback:

```csharp
public class MyProcessor : ProcessorNode, IProcessorLifecycle
{
    public void OnInit(ProcessorLifecycleContext context)
    {
        // Initialize resources after state stores are ready
    }

    public void OnClose(ProcessorLifecycleContext context)
    {
        // Flush buffers, close connections
        // context.ShutdownToken is cancelled on timeout
    }
}
```

## Per-Processor Metrics

Each processor node and state store emits OpenTelemetry-compatible metrics.

### Processor Node Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_streams_node_records_in_total` | Counter | Records received by node |
| `surgewave_streams_node_records_out_total` | Counter | Records emitted by node |
| `surgewave_streams_node_process_latency_ms` | Histogram | Processing latency per node |
| `surgewave_streams_node_errors_total` | Counter | Errors per node |

All metrics are tagged with `node.name` for per-node filtering.

### State Store Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_streams_store_puts_total` | Counter | Total put operations |
| `surgewave_streams_store_gets_total` | Counter | Total get operations |
| `surgewave_streams_store_deletes_total` | Counter | Total delete operations |
| `surgewave_streams_store_put_latency_ms` | Histogram | Put latency |
| `surgewave_streams_store_get_latency_ms` | Histogram | Get latency |
| `surgewave_streams_store_entries` | Gauge | Entry count (when available) |

All metrics are tagged with `store.name`.

## Backpressure & Flow Control

`BackpressureBuffer` provides a bounded buffer between the poll loop and processing with configurable overflow strategies.

### Configuration

```csharp
var config = new BackpressureConfig
{
    MaxBufferedRecords = 10_000,
    Strategy = BackpressureStrategy.Block,
    MaxWaitTime = TimeSpan.FromSeconds(5),
    PauseConsumerOnHighWatermark = true,
    HighWatermarkRatio = 0.8,
    LowWatermarkRatio = 0.5
};
```

### Strategies

| Strategy | Behavior |
|----------|----------|
| `Block` | Block the producer until space is available (up to `MaxWaitTime`) |
| `DropOldest` | Drop the oldest record in the buffer when full |
| `DropNewest` | Drop the incoming record when the buffer is full |

When `PauseConsumerOnHighWatermark` is enabled, the consumer pauses polling when the buffer reaches 80% capacity and resumes at 50%.

## Retry & Backoff Integration

Add automatic retry logic to stream processing with configurable backoff strategies.

### Quick Usage

```csharp
stream
    .WithRetry(maxRetries: 3)
    .MapValues(order => ProcessOrder(order))
    .To("processed-orders");
```

### Configuration

```csharp
var retryConfig = new StreamsRetryConfig
{
    Enabled = true,
    MaxRetries = 3,
    InitialDelay = TimeSpan.FromMilliseconds(100),
    MaxDelay = TimeSpan.FromSeconds(5),
    BackoffStrategy = BackoffStrategy.ExponentialWithJitter,
    ShouldRetry = ex => ex is not ArgumentException
};
```

### Backoff Strategies

| Strategy | Behavior |
|----------|----------|
| `Fixed` | Constant delay between retries |
| `Exponential` | Delay doubles each retry |
| `ExponentialWithJitter` | Exponential with random jitter to avoid thundering herd |
| `Linear` | Delay increases linearly with each retry |

The retry policy records `surgewave_streams_retry_total` and `surgewave_streams_retry_exhausted_total` metrics.

## Next Steps

- [Schema Registry](schema-registry.md) - Schema management for streams
- [Transactions](transactions.md) - Exactly-once stream processing
- [Interactive Queries](../interactive-queries.md) - Query state stores over REST
- [Circuit Breaker](../circuit-breaker.md) - Resilience for external calls in processors

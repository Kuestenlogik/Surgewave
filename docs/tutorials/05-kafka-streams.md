# Tutorial 05: Kafka Streams

Build a real-time stream processing application with filtering, aggregation, windowing, and joins.

## Prerequisites

- Completed [Tutorial 02](02-producers-consumers.md) or have a Surgewave broker running on `localhost:9092`
- .NET 10 SDK installed

## What You Will Build

A real-time analytics pipeline that:
1. Reads order events from a topic
2. Filters for large orders (above $100)
3. Aggregates revenue by product in 5-minute windows
4. Enriches orders with customer data via a stream-table join
5. Outputs results to downstream topics

## Step 1: Create the Project

```bash
mkdir surgewave-streams-tutorial && cd surgewave-streams-tutorial
dotnet new console -n StreamsApp
cd StreamsApp
dotnet add package Kuestenlogik.Surgewave.Client
dotnet add package Kuestenlogik.Surgewave.Streams
```

## Step 2: Define Data Models

Create `Models.cs`:

```csharp
namespace StreamsApp;

public record Order(string OrderId, string CustomerId, string Product, decimal Amount);
public record OrderSummary(string OrderId, decimal Amount);
public record Customer(string CustomerId, string Name, string Region);
public record EnrichedOrder(Order Order, Customer Customer);
public record ProductRevenue(string Product, decimal TotalRevenue, int OrderCount);
```

## Step 3: Build a Simple Topology

Replace `Program.cs` with a filter-and-route topology:

```csharp
using Kuestenlogik.Surgewave.Streams;
using StreamsApp;

var builder = new StreamsBuilder();

// Read orders from the "orders" topic
var orders = builder.Stream<string, Order>("orders");

// Filter large orders and write to a separate topic
orders
    .Filter((key, order) => order.Amount > 100)
    .MapValues(order => new OrderSummary(order.OrderId, order.Amount))
    .To("large-orders");

// Route small orders elsewhere
orders
    .Filter((key, order) => order.Amount <= 100)
    .To("small-orders");

// Log every order (side-effect without modifying the stream)
orders.Peek((key, order) =>
    Console.WriteLine($"Processing: {order.OrderId} - ${order.Amount}"));

// Build and configure the topology
var topology = builder.Build();

var config = new StreamsConfig
{
    ApplicationId = "order-router",
    BootstrapServers = "localhost:9092",
    DefaultKeySerde = Serdes.String(),
    DefaultValueSerde = Serdes.Json<Order>()
};

// Run the streams application
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

Console.WriteLine("Streams app started. Press Ctrl+C to stop.");
await topology.StartAsync(config, cts.Token);
```

## Step 4: Add Windowed Aggregation

Extend the topology with a 5-minute tumbling window to compute revenue per product:

```csharp
// Count and sum revenue per product in 5-minute windows
orders
    .GroupBy<string>((key, order) => order.Product)
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(5)))
    .Aggregate(
        initializer: () => new ProductRevenue("", 0m, 0),
        aggregator: (product, order, current) =>
            new ProductRevenue(
                product,
                current.TotalRevenue + order.Amount,
                current.OrderCount + 1),
        materialized: Materialized.As<string, ProductRevenue>("revenue-by-product"));
```

This creates a state store named `revenue-by-product` that you can query for current window values.

## Step 5: Stream-Table Join

Enrich orders with customer information using a table lookup:

```csharp
// Build a table from the customers changelog topic
var customers = builder.Table<string, Customer>("customers");

// Join orders (keyed by CustomerId) with the customers table
var enriched = orders
    .SelectKey((key, order) => order.CustomerId)  // Re-key by CustomerId
    .Join(
        customers,
        (order, customer) => new EnrichedOrder(order, customer));

enriched
    .Peek((key, enrichedOrder) =>
        Console.WriteLine(
            $"Order {enrichedOrder.Order.OrderId} from " +
            $"{enrichedOrder.Customer.Name} ({enrichedOrder.Customer.Region})"))
    .To("enriched-orders");
```

## Step 6: Branch by Priority

Split a stream into multiple branches based on predicates:

```csharp
var branches = orders.Branch(
    (key, order) => order.Amount > 500,   // High value
    (key, order) => order.Amount > 100,   // Medium value
    (key, order) => true                  // Everything else
);

branches[0].To("high-value-orders");
branches[1].To("medium-value-orders");
branches[2].To("standard-orders");
```

## Step 7: Stream-Stream Join

Join two streams within a time window. For example, match orders with payments:

```csharp
var orderStream = builder.Stream<string, Order>("orders");
var paymentStream = builder.Stream<string, Payment>("payments");

var matched = orderStream.Join(
    paymentStream,
    (order, payment) => new OrderPayment(order, payment),
    JoinWindows.Of(TimeSpan.FromHours(1))  // Match within 1 hour
);

matched.To("matched-orders");
```

## Complete Example

Here is a complete `Program.cs` combining filter, aggregate, and join:

```csharp
using Kuestenlogik.Surgewave.Streams;
using StreamsApp;

var builder = new StreamsBuilder();

// Source streams and tables
var orders = builder.Stream<string, Order>("orders");
var customers = builder.Table<string, Customer>("customers");

// 1. Route large orders
orders
    .Filter((key, order) => order.Amount > 100)
    .To("large-orders");

// 2. Aggregate revenue by product (5-min windows)
orders
    .GroupBy<string>((key, order) => order.Product)
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(5)))
    .Aggregate(
        initializer: () => new ProductRevenue("", 0m, 0),
        aggregator: (product, order, current) =>
            new ProductRevenue(product, current.TotalRevenue + order.Amount,
                current.OrderCount + 1),
        materialized: Materialized.As<string, ProductRevenue>("revenue-store"));

// 3. Enrich with customer data
orders
    .SelectKey((key, order) => order.CustomerId)
    .Join(customers,
        (order, customer) => new EnrichedOrder(order, customer))
    .To("enriched-orders");

// Run
var topology = builder.Build();
var config = new StreamsConfig
{
    ApplicationId = "order-analytics",
    BootstrapServers = "localhost:9092"
};

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

Console.WriteLine("Order analytics pipeline started.");
await topology.StartAsync(config, cts.Token);
```

## Window Types Reference

| Window | Use Case | Example |
|--------|----------|---------|
| **Tumbling** | Fixed, non-overlapping intervals | 5-min revenue totals |
| **Hopping** | Fixed, overlapping intervals | 5-min window, advancing every 1 min |
| **Sliding** | Time-difference based | Events within 10 min of each other |
| **Session** | Inactivity-gap based | User session with 30-min timeout |

```csharp
// Tumbling: [0-5), [5-10), [10-15) ...
.WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(5)))

// Hopping: [0-5), [1-6), [2-7) ... (advance every 1 min)
.WindowedBy(HoppingWindows.Of(TimeSpan.FromMinutes(5))
    .AdvanceBy(TimeSpan.FromMinutes(1)))

// Sliding: events within 5 min of each other
.WindowedBy(SlidingWindows.Of(TimeSpan.FromMinutes(5)))

// Session: 30-min inactivity gap
.WindowedBy(SessionWindows.With(TimeSpan.FromMinutes(30)))
```

## Adding Retry Logic

Protect against transient failures in processing:

```csharp
orders
    .WithRetry(maxRetries: 3)
    .MapValues(order => ProcessOrder(order))
    .To("processed-orders");
```

For fine-grained control:

```csharp
var retryConfig = new StreamsRetryConfig
{
    MaxRetries = 3,
    InitialDelay = TimeSpan.FromMilliseconds(100),
    MaxDelay = TimeSpan.FromSeconds(5),
    BackoffStrategy = BackoffStrategy.ExponentialWithJitter
};
```

## Next Steps

- [Tutorial 06: Schema Registry](06-schema-registry.md) -- manage schemas for your streams
- [Streams Reference](../features/streams.md) -- full API documentation
- [State Stores](../features/streams.md#state-stores) -- persistent and in-memory stores

# Recipe: Streams Topology

Copy-paste topologies for common stream processing patterns.

---

## Minimal Setup

```csharp
var config = new StreamsConfig
{
    ApplicationId = "my-app",
    BootstrapServers = "localhost:9092",
    StateDir = "./state"
};

var builder = new StreamsBuilder();

// ... define topology ...

var topology = builder.Build();
var app = new StreamsApplication(config, topology, loggerFactory);
await app.StartAsync(cts.Token);
await app.WaitForShutdownAsync();
```

---

## 1. Simple Filter / Map

Route high-value orders to a separate topic, tag each with a label.

```csharp
var builder = new StreamsBuilder();

var orders = builder.Stream<string, Order>("orders");

orders
    .Filter((key, order) => order.Amount > 1000)
    .MapValues(order => order with { Tag = "high-value" })
    .To("high-value-orders");

orders
    .FilterNot((key, order) => order.Amount > 1000)
    .To("standard-orders");

var topology = builder.Build();
```

---

## 2. Aggregation with State Store

Count orders per customer, expose results via a KTable.

```csharp
var builder = new StreamsBuilder();

var orders = builder.Stream<string, Order>("orders");

var orderCounts = orders
    .SelectKey((_, order) => order.CustomerId)   // rekey by customer
    .GroupByKey()
    .Count(Materialized.As<string, long>("order-count-store"));

// Write count updates to a topic
orderCounts.ToStream().To("order-counts");

var topology = builder.Build();
```

Query the state store at runtime:

```csharp
// GET /api/streams/stores/order-count-store/entries/{customerId}
var count = await storeRegistry.GetAsync<string, long>("order-count-store", customerId);
```

---

## 3. Stream–Table Join (Enrich Orders with Customer Data)

```csharp
var builder = new StreamsBuilder();

// Changelog topic — keyed by customer ID
var customers = builder.Table<string, Customer>("customers");

// Event stream — keyed by order ID
var orders = builder.Stream<string, Order>("orders");

var enriched = orders
    .SelectKey((_, order) => order.CustomerId)   // match table key
    .Join(
        customers,
        (order, customer) => new EnrichedOrder(order, customer));

enriched.To("enriched-orders");

var topology = builder.Build();
```

---

## 4. Windowed Aggregation — 5-Minute Tumbling Window

Sum revenue per product in 5-minute non-overlapping windows.

```csharp
var builder = new StreamsBuilder();

var sales = builder.Stream<string, SaleEvent>("sales");

sales
    .SelectKey((_, sale) => sale.ProductId)
    .GroupByKey()
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(5)))
    .Aggregate(
        initializer: () => 0m,
        aggregator: (productId, sale, total) => total + sale.Amount,
        materialized: Materialized.As<string, decimal>("revenue-5m"))
    .ToStream()
    .MapValues((windowKey, revenue) => new RevenueWindow
    {
        ProductId = windowKey.Key,
        WindowStart = windowKey.Window.Start,
        WindowEnd = windowKey.Window.End,
        Revenue = revenue
    })
    .To("revenue-windows");

var topology = builder.Build();
```

---

## 5. Exactly-Once Processing

```csharp
var config = new StreamsConfig
{
    ApplicationId = "payment-processor",
    BootstrapServers = "localhost:9092",
    ProcessingGuarantee = ProcessingGuarantee.ExactlyOnce,
    TransactionalIdPrefix = "payment-processor"
};

var builder = new StreamsBuilder();

builder.Stream<string, Payment>("payments")
    .Filter((_, p) => p.Status == "approved")
    .MapValues(p => new Ledger(p))
    .To("ledger");

var topology = builder.Build();
```

---

## 6. Branch — Fan-Out by Condition

```csharp
var builder = new StreamsBuilder();

var events = builder.Stream<string, Event>("events");

var branches = events.Branch(
    (_, e) => e.Severity == "critical",
    (_, e) => e.Severity == "warning",
    (_, _) => true   // default: info
);

branches[0].To("alerts-critical");
branches[1].To("alerts-warning");
branches[2].To("alerts-info");

var topology = builder.Build();
```

---

## 7. Retry on Failure

```csharp
var config = new StreamsConfig
{
    ApplicationId = "resilient-app",
    BootstrapServers = "localhost:9092",
    Retry = new StreamsRetryConfig
    {
        Enabled = true,
        MaxRetries = 3,
        InitialDelay = TimeSpan.FromMilliseconds(100),
        MaxDelay = TimeSpan.FromSeconds(5),
        BackoffStrategy = BackoffStrategy.ExponentialWithJitter
    }
};

var builder = new StreamsBuilder();

builder.Stream<string, Order>("orders")
    .WithRetry(maxRetries: 3)
    .MapValues(order => CallExternalService(order))
    .To("processed-orders");
```

---

## See Also

- [Streams Feature Reference](../features/streams.md)
- [Interactive Queries](../interactive-queries.md)
- [Transactions](../features/transactions.md)

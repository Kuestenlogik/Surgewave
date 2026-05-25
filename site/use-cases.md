---
layout: page
title: Use Cases
subtitle: Where teams reach for Surgewave instead of Kafka.
description: Concrete scenarios where Surgewave delivers — event-driven microservices, IoT, edge, AI pipelines, and more.
permalink: /use-cases/
---

Surgewave is designed for scenarios where you need Kafka's capabilities without its operational complexity. Here are the most common use cases where Surgewave excels.

---

## Event-Driven Microservices

Decouple services with reliable, ordered event streaming.

### Order Processing Pipeline

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Order   │───►│ Payment  │───►│ Inventory│───►│ Shipping │
│ Service  │    │ Service  │    │ Service  │    │ Service  │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
      │              │              │              │
      ▼              ▼              ▼              ▼
   orders        payments       inventory      shipments
   (topic)        (topic)        (topic)        (topic)
```

```csharp
// Order Service publishes order events
await producer.ProduceAsync("orders", order.Id, new OrderCreated
{
    OrderId = order.Id,
    CustomerId = customer.Id,
    Items = order.Items,
    Total = order.Total
});

// Payment Service consumes and processes
consumer.Subscribe("orders");
while (!cancellationToken.IsCancellationRequested)
{
    var record = await consumer.ConsumeAsync(cancellationToken);
    if (record == null) continue;
    var payment = await ProcessPayment(record.Value);
    await producer.ProduceAsync("payments", record.Key, payment);
}
```

**Why Surgewave?**
- Sub-millisecond latency for time-sensitive order processing
- Exactly-once semantics with transactions
- Easy local development with embedded mode

---

## Real-Time Analytics

Stream processing for dashboards, metrics, and alerts.

### Click Stream Analysis

```csharp
var builder = new StreamsBuilder();

// Count clicks per page in 5-minute windows
builder.Stream<string, ClickEvent>("clicks")
    .GroupBy((key, click) => click.PageUrl)
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(5)))
    .Count()
    .ToStream()
    .To("page-views");

// Alert on anomalies
builder.Stream<string, ClickEvent>("clicks")
    .GroupBy((key, click) => click.UserId)
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(1)))
    .Count()
    .Filter((key, count) => count > 1000)  // Unusual activity
    .ToStream()
    .To("alerts");
```

**Why Surgewave?**
- Native Kafka Streams implementation
- lower latency for real-time dashboards
- Built-in windowing and aggregation

---

## IoT Data Ingestion

Handle high-volume sensor data from edge devices.

### Sensor Data Pipeline

```
┌─────────┐  ┌─────────┐  ┌─────────┐
│ Sensor  │  │ Sensor  │  │ Sensor  │
│  Edge   │  │  Edge   │  │  Edge   │
└────┬────┘  └────┬────┘  └────┬────┘
     │            │            │
     └────────────┼────────────┘
                  ▼
            ┌──────────┐
            │  Surgewave   │
            │  Broker  │
            └────┬─────┘
                 │
     ┌───────────┼───────────┐
     ▼           ▼           ▼
  raw-data   aggregated   alerts
  (cold)     (warm)       (hot)
```

```csharp
// High-throughput ingestion
await using var producer = new SurgewaveProducer<string, byte[]>(options =>
{
    options.BootstrapServers = "surgewave:9092";
    options.BatchSize = 16384;
    options.LingerMs = 5;  // Batch for efficiency
});

// Send sensor readings
await producer.ProduceAsync("sensor-data", sensorId, readings);
```

**Why Surgewave?**
- 1.25M messages/second throughput
- Tiered storage for cost-effective retention (hot/warm/cold)
- Shared memory IPC for edge deployments (ultra-low latency (target))

---

## Log Aggregation

Centralize logs from distributed systems.

### Centralized Logging

```csharp
// Application logging via Kafka Connect
public class KafkaLogSink : ILogEventSink
{
    public void Emit(LogEvent logEvent)
    {
        _producer.Produce("logs", new Message<string, LogEntry>
        {
            Key = logEvent.Properties["ServiceName"].ToString(),
            Value = new LogEntry
            {
                Timestamp = logEvent.Timestamp,
                Level = logEvent.Level.ToString(),
                Message = logEvent.RenderMessage(),
                Exception = logEvent.Exception?.ToString()
            }
        });
    }
}

// Aggregation with Streams
builder.Stream<string, LogEntry>("logs")
    .Filter((service, log) => log.Level == "Error")
    .GroupByKey()
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(1)))
    .Count()
    .Filter((key, count) => count > 10)  // Error spike
    .ToStream()
    .To("error-alerts");
```

**Why Surgewave?**
- Log compaction for efficient storage
- Connect integration with Elasticsearch, S3
- High-throughput for busy systems

---

## Change Data Capture (CDC)

Stream database changes to downstream systems.

### Database Sync Pipeline

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Primary    │────►│    Surgewave     │────►│   Search     │
│   Database   │ CDC │    Broker    │     │   Index      │
└──────────────┘     └──────────────┘     └──────────────┘
                            │
                            ├────────────►  Cache
                            │
                            └────────────►  Analytics
```

```csharp
// Database connector captures changes
var connector = new DatabaseSourceConnector
{
    ConnectionString = "Server=db;Database=orders;",
    Tables = ["orders", "customers", "products"],
    Mode = CdcMode.Debezium
};

// Downstream consumers react to changes
builder.Stream<string, ChangeEvent>("db.orders")
    .Filter((key, change) => change.Operation != "DELETE")
    .MapValues(change => change.After)
    .To("orders-search-index");
```

**Why Surgewave?**
- Kafka Connect with database connectors
- Schema Registry for schema evolution
- Exactly-once delivery to downstream systems

---

## Integration Testing

Embedded broker for fast, reliable tests.

### Test with Embedded Surgewave

```csharp
public class OrderServiceTests : IAsyncLifetime
{
    private EmbeddedSurgewave _surgewave;
    private ISurgewaveClient _client;

    public async Task InitializeAsync()
    {
        // In-memory broker starts in milliseconds
        _surgewave = new EmbeddedSurgewave(options =>
        {
            options.Storage = StorageBackend.Memory;
            options.AutoCreateTopics = true;
        });
        await _surgewave.StartAsync();

        _client = new SurgewaveNativeClient("localhost", _surgewave.Port);
        await _client.ConnectAsync();
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _surgewave.DisposeAsync();
    }

    [Fact]
    public async Task OrderCreated_PublishesEvent()
    {
        // Arrange
        var orderService = new OrderService(_client);

        // Act
        var orderId = await orderService.CreateOrder(new CreateOrderRequest
        {
            CustomerId = "cust-123",
            Items = [new OrderItem { ProductId = "prod-1", Quantity = 2 }]
        });

        // Assert - verify event was published
        var message = await _client.Messaging.ConsumeOneAsync("order-events");
        var orderEvent = JsonSerializer.Deserialize<OrderCreated>(message.Value);

        Assert.Equal(orderId, orderEvent.OrderId);
        Assert.Equal("cust-123", orderEvent.CustomerId);
    }

    [Fact]
    public async Task OrderProcessor_HandlesHighVolume()
    {
        // Produce 10,000 orders
        var tasks = Enumerable.Range(0, 10000)
            .Select(i => _client.Messaging.Send("orders")
                .WithKey($"order-{i}")
                .WithValue(CreateOrderPayload(i))
                .ExecuteAsync());

        await Task.WhenAll(tasks);

        // Verify all processed
        var processor = new OrderProcessor(_client);
        await processor.ProcessAllAsync();

        Assert.Equal(10000, processor.ProcessedCount);
    }
}
```

**Why Surgewave?**
- No external dependencies
- Sub-second startup
- Identical API to production
- Deterministic tests

---

## Event Sourcing

Store application state as a sequence of events.

### Event Store Pattern

```csharp
public class AccountAggregate
{
    public string AccountId { get; private set; }
    public decimal Balance { get; private set; }
    public List<IDomainEvent> PendingEvents { get; } = new();

    public void Deposit(decimal amount)
    {
        if (amount <= 0) throw new ArgumentException("Amount must be positive");

        Apply(new MoneyDeposited
        {
            AccountId = AccountId,
            Amount = amount,
            Timestamp = DateTime.UtcNow
        });
    }

    public void Withdraw(decimal amount)
    {
        if (amount > Balance) throw new InsufficientFundsException();

        Apply(new MoneyWithdrawn
        {
            AccountId = AccountId,
            Amount = amount,
            Timestamp = DateTime.UtcNow
        });
    }

    private void Apply(IDomainEvent @event)
    {
        When(@event);
        PendingEvents.Add(@event);
    }

    private void When(IDomainEvent @event)
    {
        switch (@event)
        {
            case MoneyDeposited e: Balance += e.Amount; break;
            case MoneyWithdrawn e: Balance -= e.Amount; break;
        }
    }
}

// Event Store backed by Surgewave
public class SurgewaveEventStore : IEventStore
{
    private readonly ISurgewaveProducer<string, IDomainEvent> _producer;

    public async Task SaveAsync(AggregateRoot aggregate)
    {
        foreach (var @event in aggregate.PendingEvents)
        {
            await _producer.ProduceAsync(
                $"events-{aggregate.GetType().Name}",
                aggregate.Id,
                @event);
        }
    }

    public async Task<T> LoadAsync<T>(string id) where T : AggregateRoot, new()
    {
        var aggregate = new T();

        await foreach (var record in _consumer.ConsumeFromBeginningAsync($"events-{typeof(T).Name}"))
        {
            if (record.Key == id)
            {
                aggregate.Replay(record.Value);
            }
        }

        return aggregate;
    }
}
```

**Why Surgewave?**
- Log compaction for snapshot optimization
- Guaranteed ordering per partition
- Transactions for atomic multi-stream writes

---

## Saga / Distributed Transactions

Coordinate long-running business processes across services.

### Order Fulfillment Saga

```csharp
public class OrderSaga
{
    public async Task ExecuteAsync(Order order)
    {
        try
        {
            // Step 1: Reserve inventory
            await _producer.ProduceAsync("inventory-commands", order.Id,
                new ReserveInventory { OrderId = order.Id, Items = order.Items });

            var reserved = await WaitForEvent<InventoryReserved>("inventory-events", order.Id);

            // Step 2: Process payment
            await _producer.ProduceAsync("payment-commands", order.Id,
                new ProcessPayment { OrderId = order.Id, Amount = order.Total });

            var paid = await WaitForEvent<PaymentProcessed>("payment-events", order.Id);

            // Step 3: Ship order
            await _producer.ProduceAsync("shipping-commands", order.Id,
                new ShipOrder { OrderId = order.Id, Address = order.ShippingAddress });

            var shipped = await WaitForEvent<OrderShipped>("shipping-events", order.Id);

            // Success!
            await _producer.ProduceAsync("order-events", order.Id,
                new OrderCompleted { OrderId = order.Id });
        }
        catch (SagaStepFailedException ex)
        {
            // Compensate: reverse completed steps
            await CompensateAsync(order, ex.FailedStep);
        }
    }

    private async Task CompensateAsync(Order order, string failedStep)
    {
        if (failedStep == "shipping" || failedStep == "payment")
        {
            await _producer.ProduceAsync("payment-commands", order.Id,
                new RefundPayment { OrderId = order.Id });
        }

        if (failedStep == "shipping" || failedStep == "payment" || failedStep == "inventory")
        {
            await _producer.ProduceAsync("inventory-commands", order.Id,
                new ReleaseInventory { OrderId = order.Id });
        }

        await _producer.ProduceAsync("order-events", order.Id,
            new OrderFailed { OrderId = order.Id, Reason = failedStep });
    }
}
```

**Why Surgewave?**
- Exactly-once with transactions
- Consumer groups for reliable step execution
- Event replay for debugging failed sagas

---

## Command Query Responsibility Segregation (CQRS)

Separate read and write models with event-driven synchronization.

### CQRS Architecture

```
┌─────────────────┐                    ┌─────────────────┐
│   Write Side    │                    │   Read Side     │
│   (Commands)    │                    │   (Queries)     │
│                 │                    │                 │
│  ┌───────────┐  │     events         │  ┌───────────┐  │
│  │  Domain   │──┼────────────────────┼─►│  Read     │  │
│  │  Model    │  │        Surgewave       │  │  Model    │  │
│  └───────────┘  │                    │  └───────────┘  │
│        │        │                    │        │        │
│        ▼        │                    │        ▼        │
│  ┌───────────┐  │                    │  ┌───────────┐  │
│  │  Event    │  │                    │  │  Search   │  │
│  │  Store    │  │                    │  │  Index    │  │
│  └───────────┘  │                    │  └───────────┘  │
└─────────────────┘                    └─────────────────┘
```

```csharp
// Write side: handle commands, emit events
public class ProductCommandHandler
{
    public async Task Handle(UpdatePrice command)
    {
        var product = await _repository.GetAsync(command.ProductId);
        product.UpdatePrice(command.NewPrice);

        await _eventStore.SaveAsync(product);  // Writes to Surgewave
    }
}

// Read side: project events to queryable model
public class ProductProjection
{
    public async Task ProjectAsync()
    {
        await foreach (var @event in _consumer.ConsumeAsync("product-events"))
        {
            switch (@event.Value)
            {
                case PriceUpdated e:
                    await _searchIndex.UpdateAsync(e.ProductId, doc =>
                    {
                        doc.Price = e.NewPrice;
                        doc.LastUpdated = e.Timestamp;
                    });
                    break;
            }
        }
    }
}
```

**Why Surgewave?**
- Reliable event delivery between sides
- Consumer groups for scaled projections
- Log compaction for efficient replay

---

# Industry Use Cases

Real-world scenarios across industries where Surgewave solves critical problems.

---

## Financial Services: Trading & Market Data

### Problem
Trading systems require **microsecond-level latency** for market data distribution and order execution. Traditional message brokers introduce milliseconds of delay, causing missed opportunities and regulatory issues (MiFID II requires timestamp precision).

### Solution

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Market    │────►│    Surgewave    │────►│  Trading    │
│    Feed     │     │  (native)   │     │   Engine    │
└─────────────┘     └─────────────┘     └─────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
      Risk Mgmt      Compliance      Analytics
```

```csharp
// Market data with shared memory for co-located systems
await using var client = new SurgewaveNativeClient(
    "surgewave", 9092, SurgewaveTransportType.SharedMemory);  // ultra-low latency (target)
await client.ConnectAsync();

// Tick-by-tick market data
await foreach (var tick in consumer.ConsumeAsync("market-data"))
{
    var quote = tick.Value;

    // React within microseconds
    if (ShouldTrade(quote))
    {
        await producer.ProduceAsync("orders", new Order
        {
            Symbol = quote.Symbol,
            Price = quote.Bid,
            Quantity = CalculateSize(quote),
            Timestamp = DateTimeOffset.UtcNow  // Regulatory timestamp
        });
    }
}
```

**Problems Solved:**
- **Latency**:lower latency than Kafka wire (Kafka) = much lower latency reaction time
- **Regulatory compliance**: Precise timestamps for audit trail
- **Co-location**: Shared memory IPC for same-rack deployments
- **Throughput**: Handle 1M+ ticks/second during market open

---

## Simulation & Digital Twins

### Problem
Simulations (physics, traffic, games) require **deterministic, ordered event processing** with the ability to **replay scenarios** for debugging or what-if analysis. Traditional systems lose event ordering or can't replay reliably.

### Solution

```
┌─────────────────────────────────────────────────────────┐
│                 Simulation Engine                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Physics  │  │   AI     │  │ Renderer │              │
│  │  Engine  │  │ Agents   │  │          │              │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘              │
│       │             │             │                     │
│       └─────────────┼─────────────┘                     │
│                     ▼                                   │
│              ┌──────────────┐                          │
│              │    Surgewave     │  ◄── Deterministic       │
│              │  Event Bus   │      Ordering            │
│              └──────────────┘                          │
└─────────────────────────────────────────────────────────┘
                     │
                     ▼
              ┌──────────────┐
              │ Event Store  │  ◄── Full Replay
              │  (Tiered)    │      Capability
              └──────────────┘
```

```csharp
// Simulation tick processing
public class SimulationEngine
{
    private long _tick = 0;

    public async Task RunAsync()
    {
        await foreach (var evt in _consumer.ConsumeAsync("sim-events"))
        {
            // Events arrive in strict order per partition (entity)
            var simEvent = evt.Value;

            // Deterministic processing
            var result = ProcessEvent(simEvent, _tick);

            // Emit consequences
            foreach (var consequence in result.NewEvents)
            {
                await _producer.ProduceAsync("sim-events",
                    consequence.EntityId,  // Partition key = entity
                    consequence);
            }

            _tick++;
        }
    }

    // Replay from any point in time
    public async Task ReplayFromAsync(long fromTick)
    {
        ResetState();
        _tick = fromTick;

        // Surgewave preserves exact event order
        await foreach (var evt in _consumer.ConsumeFromOffsetAsync("sim-events", fromTick))
        {
            ProcessEvent(evt.Value, _tick++);
        }
    }
}

// Digital twin: real-world → simulation sync
public class DigitalTwinSync
{
    public async Task SyncAsync()
    {
        // Physical sensors
        await foreach (var sensor in _consumer.ConsumeAsync("iot-sensors"))
        {
            // Update digital twin state
            await _producer.ProduceAsync("twin-state", sensor.Key, new TwinUpdate
            {
                EntityId = sensor.Key,
                RealWorldState = sensor.Value,
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }
}
```

**Problems Solved:**
- **Determinism**: Guaranteed ordering enables reproducible simulations
- **Replay**: Debug issues by replaying exact event sequence
- **Scale**: Partition by entity for parallel simulation of millions of objects
- **Persistence**: Tiered storage keeps years of simulation history cost-effectively

---

## Gaming: Multiplayer State Sync

### Problem
Multiplayer games need **sub-10ms state synchronization** across players. High latency causes rubber-banding, desync, and poor player experience. Traditional solutions (custom UDP) are complex to build reliably.

### Solution

```csharp
// Game server publishes authoritative state
public class GameServer
{
    public async Task BroadcastStateAsync()
    {
        while (_running)
        {
            var gameState = CaptureGameState();

            // Partition by game room for ordering
            await _producer.ProduceAsync(
                "game-state",
                gameState.RoomId,
                gameState);

            await Task.Delay(16);  // 60 FPS tick rate
        }
    }
}

// Player actions with client-side prediction
public class PlayerClient
{
    public async Task SendActionAsync(PlayerAction action)
    {
        // Optimistic local update
        ApplyLocalPrediction(action);

        // Send to server
        await _producer.ProduceAsync("player-actions", _roomId, action);
    }

    public async Task ReceiveStateAsync()
    {
        await foreach (var state in _consumer.ConsumeAsync("game-state"))
        {
            // Reconcile with predictions
            ReconcileState(state.Value);
        }
    }
}
```

**Problems Solved:**
- **Latency**: Native protocol achieves sub-millisecond delivery
- **Ordering**: Per-room partitioning ensures correct event sequence
- **Scale**: Handle thousands of concurrent game rooms
- **Replay**: Record matches for anti-cheat analysis or spectating

---

## Healthcare: Patient Monitoring

### Problem
Patient monitoring requires **reliable, ordered delivery** of vital signs with **zero data loss**. Systems must handle sensor spikes during emergencies and maintain **audit trails** for regulatory compliance (HIPAA).

### Solution

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Patient   │  │   Patient   │  │   Patient   │
│  Monitor A  │  │  Monitor B  │  │  Monitor C  │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        ▼
                  ┌──────────┐
                  │  Surgewave   │
                  │  Broker  │
                  └────┬─────┘
                       │
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
  ┌─────────┐    ┌─────────┐    ┌─────────┐
  │ Alerting│    │   EMR   │    │Analytics│
  │ Service │    │  Sync   │    │Dashboard│
  └─────────┘    └─────────┘    └─────────┘
```

```csharp
// Vital signs ingestion
public class VitalsIngestionService
{
    public async Task IngestAsync(PatientVitals vitals)
    {
        // Partition by patient for ordering
        await _producer.ProduceAsync("patient-vitals", vitals.PatientId, vitals);
    }
}

// Real-time alerting with Streams
var builder = new StreamsBuilder();

builder.Stream<string, PatientVitals>("patient-vitals")
    .Filter((patientId, vitals) => IsAbnormal(vitals))
    .Peek((patientId, vitals) => TriggerAlert(patientId, vitals))
    .To("patient-alerts");

// Abnormality detection
bool IsAbnormal(PatientVitals v) =>
    v.HeartRate < 40 || v.HeartRate > 150 ||
    v.BloodPressureSystolic > 180 ||
    v.OxygenSaturation < 90;

// HIPAA-compliant audit trail
builder.Stream<string, PatientVitals>("patient-vitals")
    .MapValues(v => new AuditRecord
    {
        PatientId = v.PatientId,
        Timestamp = v.Timestamp,
        DataHash = ComputeHash(v),
        AccessedBy = GetCurrentUser()
    })
    .To("audit-log");  // Retained with tiered storage
```

**Problems Solved:**
- **Reliability**: Exactly-once delivery ensures no missed vitals
- **Latency**: Sub-second alerts for critical conditions
- **Audit**: Immutable log for HIPAA compliance
- **Scale**: Handle hospital-wide monitoring (thousands of devices)

---

## Transportation: Fleet Management

### Problem
Fleet operators need **real-time vehicle tracking** with historical route analysis. GPS data arrives at high frequency from thousands of vehicles. Systems must handle **intermittent connectivity** and **out-of-order events**.

### Solution

```csharp
// Vehicle telemetry ingestion
public class VehicleTelemetryService
{
    public async Task ProcessAsync()
    {
        await foreach (var position in _consumer.ConsumeAsync("vehicle-gps"))
        {
            var gps = position.Value;

            // Handle out-of-order (buffered on vehicle during connectivity loss)
            if (gps.Timestamp < _lastTimestamp[gps.VehicleId])
            {
                // Late arrival - still process for history
                await _producer.ProduceAsync("vehicle-history", gps.VehicleId, gps);
                continue;
            }

            _lastTimestamp[gps.VehicleId] = gps.Timestamp;

            // Real-time position update
            await _producer.ProduceAsync("vehicle-positions", gps.VehicleId, gps);

            // Geofence check
            if (IsOutsideGeofence(gps))
            {
                await _producer.ProduceAsync("geofence-alerts", gps.VehicleId, new Alert
                {
                    VehicleId = gps.VehicleId,
                    Type = AlertType.GeofenceViolation,
                    Position = gps
                });
            }
        }
    }
}

// Route optimization with historical data
var builder = new StreamsBuilder();

builder.Stream<string, GpsPosition>("vehicle-gps")
    .GroupByKey()
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromHours(1)))
    .Aggregate(
        () => new RouteStats(),
        (vehicleId, gps, stats) => stats.Update(gps))
    .ToStream()
    .To("route-analytics");
```

**Problems Solved:**
- **Scale**: Handle 10,000+ vehicles reporting every second
- **Connectivity**: Buffered messages survive network outages
- **History**: Tiered storage retains years of route data
- **Real-time**: Live tracking dashboards with sub-second updates

---

## Manufacturing: Industrial Automation

### Problem
Factory systems generate **millions of events per minute** from PLCs, sensors, and machines. Data must be processed for **predictive maintenance**, **quality control**, and **production optimization**. Integration with legacy OPC-UA systems is required.

### Solution

```
┌─────────────────────────────────────────────────────────┐
│                    Factory Floor                         │
│  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐          │
│  │ PLC │  │Sensor│ │Robot │  │ CNC │  │Vision│          │
│  └──┬──┘  └──┬──┘  └──┬──┘  └──┬──┘  └──┬──┘          │
│     └────────┴───────┬┴───────┴────────┘               │
│                      ▼                                  │
│              ┌──────────────┐                          │
│              │  OPC-UA      │                          │
│              │  Connector   │                          │
│              └──────┬───────┘                          │
└─────────────────────┼───────────────────────────────────┘
                      ▼
                ┌──────────┐
                │  Surgewave   │
                │  Broker  │
                └────┬─────┘
                     │
     ┌───────────────┼───────────────┐
     ▼               ▼               ▼
 Predictive       Quality        Production
 Maintenance      Control        Dashboard
```

```csharp
// OPC-UA to Surgewave connector
public class OpcUaConnector : ISourceConnector
{
    public async Task PollAsync()
    {
        var nodes = await _opcClient.ReadNodesAsync(_monitoredNodes);

        foreach (var node in nodes)
        {
            await _producer.ProduceAsync("factory-events", node.NodeId, new MachineEvent
            {
                NodeId = node.NodeId,
                Value = node.Value,
                Quality = node.Quality,
                Timestamp = node.SourceTimestamp
            });
        }
    }
}

// Predictive maintenance with Streams
var builder = new StreamsBuilder();

// Detect anomalies in vibration sensors
builder.Stream<string, MachineEvent>("factory-events")
    .Filter((nodeId, evt) => nodeId.StartsWith("vibration-"))
    .GroupByKey()
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(5)))
    .Aggregate(
        () => new VibrationStats(),
        (nodeId, evt, stats) => stats.Update(evt.Value))
    .Filter((key, stats) => stats.IsAnomaly())
    .ToStream()
    .MapValues(stats => new MaintenanceAlert
    {
        MachineId = stats.MachineId,
        PredictedFailure = stats.PredictedFailureTime,
        Confidence = stats.Confidence
    })
    .To("maintenance-alerts");

// Quality control: correlate multiple sensors
builder.Stream<string, MachineEvent>("factory-events")
    .Filter((nodeId, evt) => nodeId.StartsWith("quality-"))
    .GroupBy((nodeId, evt) => evt.BatchId)  // Group by production batch
    .WindowedBy(SessionWindows.With(TimeSpan.FromMinutes(30)))
    .Aggregate(
        () => new BatchQuality(),
        (batchId, evt, quality) => quality.AddMeasurement(evt))
    .ToStream()
    .Filter((key, quality) => !quality.PassesSpec())
    .To("quality-alerts");
```

**Problems Solved:**
- **Throughput**: Handle millions of sensor readings per minute
- **Latency**: Real-time alerts for machine failures
- **Integration**: Connect to legacy OPC-UA, Modbus, MQTT systems
- **Analytics**: Stream processing for predictive maintenance

---

## Telecommunications: Call Detail Records

### Problem
Telcos process **billions of CDRs daily** for billing, fraud detection, and network analytics. Data arrives from distributed network elements and must be **deduplicated**, **enriched**, and **aggregated** in real-time.

### Solution

```csharp
// CDR ingestion from network elements
public class CdrIngestionService
{
    public async Task IngestAsync(RawCdr cdr)
    {
        // Deduplicate based on call ID
        var key = $"{cdr.CallId}-{cdr.SequenceNumber}";

        await _producer.ProduceAsync("raw-cdrs", key, cdr);
    }
}

// CDR processing pipeline
var builder = new StreamsBuilder();

// Enrich with subscriber data
var subscriberTable = builder.GlobalTable<string, Subscriber>("subscribers");

builder.Stream<string, RawCdr>("raw-cdrs")
    // Deduplicate
    .GroupByKey()
    .Reduce((existing, incoming) => existing)  // Keep first
    .ToStream()
    // Enrich
    .Join(subscriberTable,
        (key, cdr) => cdr.CallerId,
        (cdr, subscriber) => new EnrichedCdr
        {
            Cdr = cdr,
            CallerPlan = subscriber.Plan,
            CallerSegment = subscriber.Segment
        })
    .To("enriched-cdrs");

// Real-time fraud detection
builder.Stream<string, EnrichedCdr>("enriched-cdrs")
    .GroupBy((key, cdr) => cdr.Cdr.CallerId)
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromHours(1)))
    .Aggregate(
        () => new CallerStats(),
        (callerId, cdr, stats) => stats.Update(cdr))
    .Filter((key, stats) => stats.IsSuspicious())
    .ToStream()
    .To("fraud-alerts");

// Billing aggregation
builder.Stream<string, EnrichedCdr>("enriched-cdrs")
    .GroupBy((key, cdr) => $"{cdr.Cdr.CallerId}-{cdr.Cdr.BillingPeriod}")
    .Aggregate(
        () => new BillingRecord(),
        (key, cdr, billing) => billing.AddCall(cdr))
    .ToStream()
    .To("billing-records");
```

**Problems Solved:**
- **Scale**: Process billions of CDRs daily
- **Deduplication**: Handle retransmissions from network elements
- **Enrichment**: Join with subscriber data for billing
- **Real-time**: Fraud detection within minutes, not hours

---

## Media: Live Event Streaming

### Problem
Live events (sports, concerts, news) require **real-time content distribution** to millions of viewers with **synchronized metadata** (scores, captions, reactions). Spikes during popular events can 100x normal traffic.

### Solution

```csharp
// Live event metadata distribution
public class LiveEventService
{
    public async Task BroadcastAsync(string eventId, EventUpdate update)
    {
        // High-priority updates (scores, breaking news)
        if (update.Priority == Priority.High)
        {
            await _producer.ProduceAsync("live-updates-priority", eventId, update);
        }

        // Standard updates
        await _producer.ProduceAsync("live-updates", eventId, update);

        // Analytics
        await _producer.ProduceAsync("event-analytics", eventId, new AnalyticsEvent
        {
            EventId = eventId,
            UpdateType = update.Type,
            Timestamp = DateTimeOffset.UtcNow
        });
    }
}

// Viewer reaction aggregation
var builder = new StreamsBuilder();

builder.Stream<string, ViewerReaction>("viewer-reactions")
    .GroupBy((key, reaction) => $"{reaction.EventId}-{reaction.Type}")
    .WindowedBy(TumblingWindows.Of(TimeSpan.FromSeconds(10)))
    .Count()
    .ToStream()
    .To("reaction-counts");  // Feed live "pulse" visualization

// Content synchronization
builder.Stream<string, ContentSegment>("content-segments")
    .Join(
        builder.Stream<string, Metadata>("content-metadata"),
        (segment, metadata) => new SynchronizedContent
        {
            Segment = segment,
            Captions = metadata.Captions,
            Overlays = metadata.Overlays
        },
        JoinWindows.Of(TimeSpan.FromSeconds(5)))
    .To("synchronized-content");
```

**Problems Solved:**
- **Spikes**: Handle 100x traffic during popular events
- **Latency**: Sub-second delivery for live scores
- **Sync**: Coordinate video, captions, and overlays
- **Scale**: Millions of concurrent viewers

---

## Industry Summary

| Industry | Primary Problem | Surgewave Solution | Key Benefit |
|----------|-----------------|----------------|-------------|
| **Financial Trading** | Millisecond latency kills profits | low (target) native protocol, shared memory IPC | much lower latency than Kafka |
| **Simulation/Gaming** | Non-deterministic replay | Guaranteed ordering per partition | Reproducible scenarios |
| **Healthcare** | Data loss = patient harm | Exactly-once delivery, audit logging | Zero missed vitals |
| **Transportation** | Connectivity gaps lose data | Durable buffering, out-of-order handling | No lost positions |
| **Manufacturing** | Legacy system integration | OPC-UA/Modbus connectors, high throughput | Unified data platform |
| **Telecom** | Billions of CDRs daily | Stream processing, deduplication | Real-time fraud detection |
| **Media** | 100x traffic spikes | Elastic scaling, low latency | Sub-second live updates |

---

## Comparison: When to Use Surgewave

| Scenario | Surgewave | Kafka | Redis Streams |
|----------|-------|-------|---------------|
| **Microservices events** | Excellent | Good | Limited |
| **High throughput** | 1.25M msg/s | 68K msg/s | 500K msg/s |
| **Low latency** | low (target) P50 | Kafka-protocol baseline P50 | 1ms P50 |
| **Integration testing** | Embedded | Testcontainers | Embedded |
| **Operations complexity** | Low (single binary) | High (ZK/KRaft) | Low |
| **Kafka compatibility** | 100% | N/A | None |
| **Stream processing** | Kafka Streams API | Kafka Streams | Limited |
| **Schema evolution** | Schema Registry | Schema Registry | Manual |

---

## Next Steps

- [Quickstart](quickstart/index.md) - Get started in 5 minutes
- [Streams](features/streams.md) - Real-time stream processing
- [Connect](features/connect.md) - Data integration connectors
- [Samples](samples/index.md) - Code examples

# Chaos Testing

The `Kuestenlogik.Surgewave.Testing.Chaos` package provides a chaos engineering framework for testing Surgewave cluster resilience. Inject faults into storage, transport, and Raft consensus to verify that your applications handle failures gracefully.

## Overview

The framework consists of:

| Component | Purpose |
|-----------|---------|
| `ChaosEngine` | Central fault orchestrator, thread-safe |
| `ChaosCluster` | Multi-broker test cluster with per-broker fault injection |
| `FaultSchedule` | Time-delayed fault activation and auto-deactivation |
| Scenario helpers | Pre-built scenarios for common fault patterns |

## Fault Types

The `FaultType` enum defines 9 injectable faults:

| Fault Type | Description |
|------------|-------------|
| `NetworkPartition` | Prevents communication between nodes |
| `NodeCrash` | All operations fail on the target node |
| `DiskIoError` | Storage read/write operations fail |
| `SlowNetwork` | Injects configurable latency into operations |
| `MessageCorruption` | Flips bits in read data |
| `LeaderElectionDisruption` | Drops Raft vote requests |
| `StorageFullError` | Simulates a full disk (writes fail) |
| `ConnectionReset` | Simulates TCP connection resets |
| `PartialWrite` | Only some data is persisted |

## ChaosEngine

The engine manages fault activation, probability evaluation, and event timeline recording:

```csharp
var engine = new ChaosEngine();

// Activate a fault with optional scope
var faultId = engine.ActivateFault(
    FaultType.NetworkPartition,
    new FaultScope
    {
        BrokerId = 0,
        TargetPeerId = 1,
        Probability = 1.0  // Always trigger (use < 1.0 for partial faults)
    });

// Check if a fault should trigger
if (engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 0, peerId: 1))
{
    // Fault is active and probability check passed
}

// Inject latency
engine.ActivateFault(FaultType.SlowNetwork, latency: TimeSpan.FromMilliseconds(200));
var delay = engine.GetInjectedLatency(FaultType.SlowNetwork, brokerId: 0);

// Deactivate
engine.DeactivateFault(faultId);

// Deactivate all
engine.DeactivateAll();

// Review timeline
foreach (var evt in engine.Timeline.Events)
{
    Console.WriteLine($"{evt.Timestamp}: {evt.EventType} - {evt.Description}");
}
```

### Fault Scope

Each fault can be scoped to specific brokers and peers:

```csharp
var scope = new FaultScope
{
    BrokerId = 0,         // Target broker (null = all)
    TargetPeerId = 1,     // Target peer (null = all)
    Topic = "orders",     // Target topic (null = all)
    Probability = 0.5     // 50% chance of triggering per check
};
```

## ChaosCluster

A multi-broker test cluster where each broker has its own `ChaosEngine`:

```csharp
await using var cluster = await ChaosCluster.CreateAsync(brokerCount: 3);

// Create a topic
await cluster.CreateTopicAsync("test-topic", partitions: 3);

// Produce messages through broker 0
await cluster.ProduceAsync("test-topic", messageCount: 100, brokerId: 0);

// Partition broker 1 from the cluster
using var partition = cluster.PartitionBroker(brokerId: 1);

// Produce more messages (only brokers 0 and 2 can communicate)
await cluster.ProduceAsync("test-topic", messageCount: 50);

// Verify data is still consumable
var messages = await cluster.ConsumeAsync("test-topic", expectedCount: 100,
    timeout: TimeSpan.FromSeconds(10));
```

### Convenience Methods

```csharp
// Network partition
using var partition = cluster.PartitionBroker(brokerId: 1);
// partition automatically heals on dispose

// Broker crash
using var crash = cluster.CrashBroker(brokerId: 2);
// crash.Recover() to bring it back before dispose

// Latency injection
cluster.InjectLatency(brokerId: 0, TimeSpan.FromMilliseconds(100));

// Heal all faults across all brokers
cluster.HealAll();
```

### Getting Connection Info

```csharp
// Connect a Kafka client to a specific broker
var bootstrapServers = cluster.GetBootstrapServers(brokerId: 0);

// Access the runtime directly
var runtime = cluster.GetRuntime(brokerId: 0);

// Access the chaos engine for a broker
var engine = cluster.GetEngine(brokerId: 0);
```

## Scenario Helpers

### Network Partition

Isolates a broker from specific peers:

```csharp
using var partition = NetworkPartitionScenario.Create(engine, brokerId: 0, peerIds: [1, 2]);

// Verify behavior under partition...

// Heal manually before dispose
partition.Heal();
```

### Broker Crash

Simulates a complete node failure:

```csharp
using var crash = BrokerCrashScenario.Create(engine, brokerId: 1);

// All operations on broker 1 fail...

// Recover
crash.Recover();
```

### Latency Injection

Adds configurable delay to network operations:

```csharp
using var latency = LatencyInjectionScenario.Create(engine, brokerId: 0,
    latency: TimeSpan.FromMilliseconds(200));
```

### Disk Failure

Simulates storage-level failures:

```csharp
using var diskFail = DiskFailureScenario.Create(engine, brokerId: 0);
// All storage reads/writes fail
```

## Fault Scheduling

Schedule faults to activate after a delay and auto-deactivate after a duration:

```csharp
// Activate a network partition in 5 seconds, lasting 10 seconds
using var schedule = FaultSchedule.Create(
    engine,
    FaultType.NetworkPartition,
    new FaultScope { BrokerId = 0, TargetPeerId = 1 },
    activateAfter: TimeSpan.FromSeconds(5),
    duration: TimeSpan.FromSeconds(10));

// The fault activates at T+5s and deactivates at T+15s
// Disposing the schedule cancels pending activations
```

## Storage Fault Injection

The chaos framework wraps storage components:

- **ChaosLogSegment** - Wraps `ILogSegment` to inject read/write failures, corruption, and partial writes
- **ChaosLogSegmentFactory** - Wraps the segment factory to produce chaos-enabled segments

### Raft Transport Injection

- **ChaosRaftTransport** - Wraps the Raft transport to inject network partitions, latency, and message drops between cluster nodes

## Integration with SurgewaveRuntime

Use the `WithChaosEngine` extension to build a chaos-enabled broker:

```csharp
var engine = new ChaosEngine();
var runtime = await SurgewaveRuntime.CreateBuilder()
    .WithBrokerId(0)
    .WithPort(0)
    .WithStorageMode(StorageMode.Memory)
    .WithChaosEngine(engine, loggerFactory)
    .WithAutoCreateTopics(true)
    .Build()
    .StartAsync();
```

## Next Steps

- [Testing Guide](../setup/testing.md) - Running tests and benchmarks
- [Performance Regression](../performance/regression-suite.md) - Automated performance regression detection

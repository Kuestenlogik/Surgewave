# Embedded Broker

Run Surgewave in-process for testing, development, or microservice architectures.

## Overview

`SurgewaveRuntime` runs a Surgewave broker within your application process. It is the same broker the standalone
`surgewave-broker` host runs, started from code instead of configuration files.

## Installation

```bash
dotnet add package Kuestenlogik.Surgewave.Runtime
```

## Basic Usage

```csharp
using Kuestenlogik.Surgewave.Runtime;

// Build the options, start the broker
await using var broker = await SurgewaveRuntime.CreateBuilder()
    .WithPort(9092)
    .WithMemoryStorage()
    .WithAutoCreateTopics()
    .Build()
    .StartAsync();

// The broker now listens on broker.BootstrapServers ("127.0.0.1:9092").
// Use any Kafka client to connect.

// The broker stops when disposed.
```

`Build()` returns a `SurgewaveRuntimeOptions` record; `StartAsync()` on it starts the broker and returns the running
`SurgewaveRuntime`. `SurgewaveRuntime.StartAsync(options)` does the same for options built by hand.

## Configuration Options

```csharp
await using var broker = await SurgewaveRuntime.CreateBuilder()
    // Network
    .WithHost("127.0.0.1")
    .WithPort(0)                    // 0 picks a free port; read it from broker.Port
    .WithBrokerId(1)

    // Storage
    .WithMemoryStorage()            // In-memory (fast, no persistence)
    // .WithFileStorage()           // File-based (persistent)
    // .WithStorageEngine(StorageEngines.ZeroCopyWal) // Zero-copy WAL

    // Topics
    .WithAutoCreateTopics()
    .WithPartitions(3)              // Default partition count for new topics

    // Data directory (for file-based storage); removed on dispose unless WithCleanup(false)
    .WithDataDirectory(Path.Combine(Path.GetTempPath(), "surgewave-test"))
    .WithCleanup(false)

    .Build()
    .StartAsync();
```

Further options: `WithRetentionHours`, `WithRetentionBytes`, `WithSasl`, `WithTls`, `WithAcl`, `WithoutKafka` (native
protocol only), `WithLogging(loggerFactory)` and `WithShutdownTimeout(seconds)`. `ConfigureFrom(brokerConfig)` takes
the settings of an existing `BrokerConfig`.

## Integration Testing

### xUnit Example

```csharp
public class KafkaIntegrationTests : IAsyncLifetime
{
    private SurgewaveRuntime _broker = null!;

    public async ValueTask InitializeAsync()
    {
        _broker = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)            // a free port, so parallel test classes never collide
            .WithMemoryStorage()
            .Build()
            .StartAsync();
    }

    public ValueTask DisposeAsync() => _broker.DisposeAsync();

    [Fact]
    public async Task ProduceConsume_RoundTrip_Success()
    {
        // Arrange
        var config = new ProducerConfig { BootstrapServers = _broker.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(config).Build();

        // Act
        await producer.ProduceAsync("test-topic", new Message<string, string>
        {
            Key = "key",
            Value = "value"
        });

        // Assert
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _broker.BootstrapServers,
            GroupId = "test-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe("test-topic");

        var result = consumer.Consume(TimeSpan.FromSeconds(5));
        Assert.Equal("value", result.Message.Value);
    }
}
```

### NUnit Example

```csharp
[TestFixture]
public class KafkaTests
{
    private SurgewaveRuntime _broker = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        _broker = await SurgewaveRuntime.CreateBuilder().WithPort(0).WithMemoryStorage().Build().StartAsync();
    }

    [OneTimeTearDown]
    public async Task Teardown()
    {
        await _broker.DisposeAsync();
    }

    [Test]
    public async Task TestProduceConsume()
    {
        // Test implementation, connecting to _broker.BootstrapServers
    }
}
```

## ASP.NET Core Integration

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(SurgewaveRuntime.CreateBuilder()
    .WithPort(9092)
    .WithMemoryStorage()
    .Build());
builder.Services.AddHostedService<SurgewaveHostedService>();

public sealed class SurgewaveHostedService(SurgewaveRuntimeOptions options) : IHostedService
{
    private SurgewaveRuntime? _broker;

    public async Task StartAsync(CancellationToken ct) => _broker = await options.StartAsync(ct);

    public async Task StopAsync(CancellationToken ct)
    {
        if (_broker is not null)
            await _broker.DisposeAsync();
    }
}
```

## IPv4-Only Mode

In environments without IPv6 support (some Docker/CI setups), use IPv4-only mode to avoid connection issues:

```csharp
await using var runtime = await SurgewaveRuntime.CreateBuilder()
    .WithPort(0)               // Dynamic port
    .WithIPv4Only()            // Bind to 127.0.0.1 only
    .WithMemoryStorage()
    .WithAutoCreateTopics()
    .Build()
    .StartAsync();

// Connect using the IPv4 address explicitly
var bootstrapServers = $"127.0.0.1:{runtime.Port}";
```

When IPv4-only mode is enabled, the broker advertises `127.0.0.1` in metadata responses, ensuring clients don't attempt IPv6 connections.

## Clusters

Several runtimes in one process form a cluster: give each a broker id, a replication port, the other nodes and the
controller quorum.

```csharp
// ports[0..2]: client ports, ports[3..5]: replication ports, reserved up front
var quorum = "1@127.0.0.1:19192,2@127.0.0.1:19193,3@127.0.0.1:19194";
var nodes = new[] { "1:127.0.0.1:19092:19192", "2:127.0.0.1:19093:19193", "3:127.0.0.1:19094:19194" };

var brokers = new List<SurgewaveRuntime>();
for (var i = 0; i < 3; i++)
{
    brokers.Add(await SurgewaveRuntime.CreateBuilder()
        .WithBrokerId(i + 1)
        .WithPort(19092 + i)
        .WithReplicationPort(19192 + i)
        .WithCluster(nodes.Where((_, n) => n != i))
        .WithControllerQuorum(quorum)
        .WithMemoryStorage()
        .Build()
        .StartAsync());
}
```

To try a cluster by hand, `scripts/stack.ps1 start -Brokers 3` starts three broker processes instead.

## Limitations

- **No Persistence** (Memory mode) - Data lost on restart
- **Resource Sharing** - Competes with application for CPU/memory

## Best Practices

1. **Use Memory Storage for Tests** - Faster and no cleanup needed
2. **Use Port 0 in Tests** - The broker picks a free port; connect via `BootstrapServers`
3. **Dispose Properly** - Always dispose to release resources
4. **Isolate Tests** - Use unique topic names per test

## Next Steps

- [Storage Backends](../storage/index.md) - Storage configuration
- [Client APIs](../clients/index.md) - Connecting to the broker
- [Performance Tuning](../performance/tuning.md) - Optimization options

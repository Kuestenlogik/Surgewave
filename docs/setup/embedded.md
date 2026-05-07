# Embedded Broker

Run Surgewave in-process for testing, development, or microservice architectures.

## Overview

The embedded broker runs Surgewave within your application process, eliminating the need for external broker management during testing or for simple use cases.

## Installation

```bash
dotnet add package Kuestenlogik.Surgewave.Broker
```

## Basic Usage

```csharp
using Kuestenlogik.Surgewave.Broker;

// Create and start embedded broker
await using var broker = new EmbeddedSurgewave(options =>
{
    options.Port = 9092;
    options.GrpcPort = 9093;
    options.Storage = StorageBackend.Memory;
    options.AutoCreateTopics = true;
});

await broker.StartAsync();

// Broker is now running on localhost:9092
// Use any Kafka client to connect

// Broker automatically stops when disposed
```

## Configuration Options

```csharp
await using var broker = new EmbeddedSurgewave(options =>
{
    // Network
    options.Host = "localhost";
    options.Port = 9092;
    options.GrpcPort = 9093;

    // Storage
    options.Storage = StorageBackend.Memory;        // In-memory (fast, no persistence)
    // options.Storage = StorageBackend.File;       // File-based (persistent)
    // options.Storage = StorageBackend.ZeroCopyWal; // Zero-copy WAL (fastest persistent)

    // Topics
    options.AutoCreateTopics = true;
    options.DefaultNumPartitions = 3;

    // Data directory (for file-based storage)
    options.DataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-test");
});
```

## Integration Testing

### xUnit Example

```csharp
public class KafkaIntegrationTests : IAsyncLifetime
{
    private EmbeddedSurgewave _broker;

    public async Task InitializeAsync()
    {
        _broker = new EmbeddedSurgewave(options =>
        {
            options.Port = 19092;  // Use non-standard port
            options.Storage = StorageBackend.Memory;
        });
        await _broker.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _broker.DisposeAsync();
    }

    [Fact]
    public async Task ProduceConsume_RoundTrip_Success()
    {
        // Arrange
        var config = new ProducerConfig { BootstrapServers = "localhost:19092" };
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
            BootstrapServers = "localhost:19092",
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
    private EmbeddedSurgewave _broker;

    [OneTimeSetUp]
    public async Task Setup()
    {
        _broker = new EmbeddedSurgewave(o => o.Storage = StorageBackend.Memory);
        await _broker.StartAsync();
    }

    [OneTimeTearDown]
    public async Task Teardown()
    {
        await _broker.DisposeAsync();
    }

    [Test]
    public async Task TestProduceConsume()
    {
        // Test implementation
    }
}
```

## ASP.NET Core Integration

```csharp
var builder = WebApplication.CreateBuilder(args);

// Add Surgewave as a hosted service
builder.Services.AddSingleton<EmbeddedSurgewave>(sp =>
{
    return new EmbeddedSurgewave(options =>
    {
        options.Port = 9092;
        options.Storage = StorageBackend.Memory;
    });
});

builder.Services.AddHostedService<SurgewaveHostedService>();

public class SurgewaveHostedService : IHostedService
{
    private readonly EmbeddedSurgewave _broker;

    public SurgewaveHostedService(EmbeddedSurgewave broker)
    {
        _broker = broker;
    }

    public Task StartAsync(CancellationToken ct) => _broker.StartAsync();
    public Task StopAsync(CancellationToken ct) => _broker.DisposeAsync().AsTask();
}
```

## Performance Testing

```csharp
// High-performance embedded setup for benchmarks
await using var broker = new EmbeddedSurgewave(options =>
{
    options.Storage = StorageBackend.Memory;
    options.UseChannelPipeline = true;
    options.ChannelWriteWorkers = Environment.ProcessorCount;
    options.ProducerBatchSizeBytes = 64 * 1024;  // 64KB batches
});
```

## IPv4-Only Mode

In environments without IPv6 support (some Docker/CI setups), use IPv4-only mode to avoid connection issues:

```csharp
await using var runtime = await SurgewaveRuntime.CreateBuilder()
    .WithPort(0)               // Dynamic port
    .WithIPv4Only()            // Bind to 127.0.0.1 only
    .WithStorageMode(StorageMode.Memory)
    .WithAutoCreateTopics(true)
    .Build()
    .StartAsync();

// Connect using the IPv4 address explicitly
var bootstrapServers = $"127.0.0.1:{runtime.Port}";
```

When IPv4-only mode is enabled, the broker advertises `127.0.0.1` in metadata responses, ensuring clients don't attempt IPv6 connections.

## Limitations

- **Single Node Only** - Embedded broker doesn't support clustering
- **No Persistence** (Memory mode) - Data lost on restart
- **Resource Sharing** - Competes with application for CPU/memory

## Best Practices

1. **Use Memory Storage for Tests** - Faster and no cleanup needed
2. **Use Unique Ports** - Avoid conflicts with other services
3. **Dispose Properly** - Always dispose to release resources
4. **Isolate Tests** - Use unique topic names per test

## Next Steps

- [Storage Backends](../storage/index.md) - Storage configuration
- [Client APIs](../clients/index.md) - Connecting to the broker
- [Performance Tuning](../performance/tuning.md) - Optimization options

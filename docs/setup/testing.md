# Testing Guide

This guide covers running and writing tests for Surgewave.

## Running Tests

### Unit Tests

```bash
# Run all unit tests
dotnet test

# Run specific test project
dotnet test tests/Kuestenlogik.Surgewave.Core.Tests

# Run with verbose output
dotnet test --logger "console;verbosity=detailed"

# Run tests matching a pattern
dotnet test --filter "FullyQualifiedName~Producer"
```

### Integration Tests

Integration tests require Docker:

```bash
# Run integration tests
dotnet test tests/Kuestenlogik.Surgewave.IntegrationTests

# Run specific integration test
dotnet test tests/Kuestenlogik.Surgewave.IntegrationTests --filter "KRaft"
```

### Benchmarks

```bash
cd benchmarks/Kuestenlogik.Surgewave.Benchmarks

# Run all benchmarks
dotnet run -c Release

# Run specific benchmark
dotnet run -c Release -- --filter "*Producer*"

# Generate comparison report
dotnet run -c Release -- --filter "*" --exporters json
```

## Test Categories

| Category | Location | Purpose |
|----------|----------|---------|
| **Unit Tests** | `tests/Kuestenlogik.Surgewave.*.Tests/` | Test individual components |
| **Integration Tests** | `tests/Kuestenlogik.Surgewave.IntegrationTests/` | Test component interactions |
| **Kafka Compatibility** | `tests/Kuestenlogik.Surgewave.KafkaCompat.Tests/` | Verify Kafka client compatibility |
| **Benchmarks** | `benchmarks/Kuestenlogik.Surgewave.Benchmarks/` | Performance measurements |

## Writing Tests

### Unit Test Example

```csharp
public class TopicManagerTests
{
    [Fact]
    public async Task CreateTopic_WithValidConfig_Succeeds()
    {
        // Arrange
        var storage = new MemoryStorageEngine();
        var manager = new TopicManager(storage);

        // Act
        var result = await manager.CreateTopicAsync("test-topic", partitions: 3);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(3, result.Topic.PartitionCount);
    }
}
```

### Integration Test with Embedded Broker

```csharp
public class ProducerIntegrationTests : IAsyncLifetime
{
    private EmbeddedSurgewave _broker;

    public async Task InitializeAsync()
    {
        _broker = new EmbeddedSurgewave(options =>
        {
            options.Port = 19092;
            options.Storage = StorageBackend.Memory;
        });
        await _broker.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _broker.DisposeAsync();
    }

    [Fact]
    public async Task Produce_AndConsume_RoundTrip()
    {
        // Test code here
    }
}
```

### Kafka Compatibility Test

```csharp
public class KafkaClientCompatibilityTests
{
    [Fact]
    public async Task ConfluentKafka_CanProduceAndConsume()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        var result = await producer.ProduceAsync("test",
            new Message<string, string> { Key = "k", Value = "v" });

        Assert.NotNull(result);
    }
}
```

## Test Utilities

### TestFixtures

```csharp
// Use shared broker across tests
public class SharedBrokerFixture : IAsyncLifetime
{
    public EmbeddedSurgewave Broker { get; private set; }

    public async Task InitializeAsync()
    {
        Broker = new EmbeddedSurgewave();
        await Broker.StartAsync();
    }

    public async Task DisposeAsync() => await Broker.DisposeAsync();
}

// Use in test class
public class MyTests : IClassFixture<SharedBrokerFixture>
{
    private readonly SharedBrokerFixture _fixture;

    public MyTests(SharedBrokerFixture fixture) => _fixture = fixture;
}
```

## Continuous Integration

Tests run automatically on:
- Pull requests
- Pushes to main branch

### GitHub Actions

```yaml
- name: Test
  run: dotnet test --configuration Release --no-build --verbosity normal
```

## Coverage Reports

```bash
# Generate coverage report
dotnet test --collect:"XPlat Code Coverage"

# View report (requires reportgenerator)
reportgenerator -reports:**/coverage.cobertura.xml -targetdir:coverage-report
```

# Surgewave Testing Guide

This guide covers all testing aspects of Surgewave: unit tests, integration tests, benchmarks, and Kafka compatibility validation.

## Quick Start

```bash
# Build the solution
dotnet build

# Run all tests
dotnet test

# Run tests with Release configuration
dotnet build -c Release && dotnet test -c Release --no-build
```

## Test Types

### 1. Unit Tests

Unit tests cover core components without requiring external dependencies:

```bash
# Run all unit tests
dotnet test tests/Kuestenlogik.Surgewave.Tests

# Run specific test classes
dotnet test --filter "FullyQualifiedName~LogSegmentTests"
dotnet test --filter "FullyQualifiedName~PartitionLogTests"
dotnet test --filter "FullyQualifiedName~SerializationTests"
```

### 2. Integration Tests

Integration tests verify end-to-end functionality using an embedded Surgewave broker.

#### Embedded Broker Tests

These tests use `EmbeddedSurgewave` for self-contained testing:

```bash
# Run embedded broker tests
dotnet test --filter "FullyQualifiedName~EmbeddedSurgewave"
```

```csharp
// Example: Using EmbeddedSurgewave in tests
await using var surgewave = await EmbeddedSurgewave.StartAsync();
Console.WriteLine($"Broker running at {surgewave.BootstrapServers}");

// Use fluent builder for custom configuration
await using var surgewave = await EmbeddedSurgewave.CreateBuilder()
    .WithPort(9092)
    .WithPartitions(3)
    .WithAutoCreateTopics(true)
    .StartAsync();
```

#### Kafka Compatibility Tests

These tests prove Surgewave is wire-compatible with Kafka using the Confluent.Kafka client:

```bash
# Run Kafka compatibility tests (requires Surgewave broker running)
dotnet test --filter "FullyQualifiedName~ConfluentKafkaCompatibility"

# Run all compatibility tests with verbose output
dotnet test --filter "FullyQualifiedName~ConfluentKafkaCompatibility" --logger "console;verbosity=detailed"
```

**Prerequisites**: Start the Surgewave broker before running these tests:

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker
```

### 3. Protocol Tests

Tests for native protocol and Kafka wire protocol:

```bash
# Native protocol tests
dotnet test --filter "FullyQualifiedName~NativeProtocol"

# SASL authentication tests
dotnet test --filter "FullyQualifiedName~Sasl"
```

## Benchmarks

Surgewave includes comprehensive benchmarks for throughput measurement and protocol comparison.

### Self-Contained Benchmarks (No External Broker)

These benchmarks use an embedded Surgewave broker:

```bash
cd benchmarks/Kuestenlogik.Surgewave.Benchmarks

# Surgewave Native protocol throughput
dotnet run -c Release -- embedded 100000 100 1000
# Parameters: [msgCount] [msgSize] [batchSize]

# Protocol comparison (Native vs Kafka on same embedded broker)
dotnet run -c Release -- embedded-compare 100000 100 1000
```

### Testcontainers-Based Comparison

Full 4-way comparison using Docker containers managed by Testcontainers.NET:

```bash
# Full comparison: Surgewave Native, Surgewave+Kafka, Pure Kafka, Redpanda
dotnet run -c Release -- compare 100000 100 1000

# Skip specific brokers if Docker unavailable
dotnet run -c Release -- compare 100000 100 1000 --skip-kafka
dotnet run -c Release -- compare 100000 100 1000 --skip-redpanda
dotnet run -c Release -- compare 100000 100 1000 --skip-kafka --skip-redpanda
```

**Requirements**: Docker must be running for Kafka/Redpanda comparison.

### BenchmarkDotNet Micro-Benchmarks

Detailed performance analysis of individual components:

```bash
cd benchmarks/Kuestenlogik.Surgewave.Benchmarks

# Serialization performance
dotnet run -c Release -- --filter *Serialization*

# Compression algorithms (Gzip, Snappy, LZ4, Zstd)
dotnet run -c Release -- --filter *Compression*

# SIMD big-endian conversions
dotnet run -c Release -- --filter *SimdBigEndian*

# Storage I/O benchmarks
dotnet run -c Release -- --filter *Storage*
```

### Automated Benchmark Suite

Run all benchmarks and optionally update the baseline:

```powershell
cd benchmarks

# Run all benchmarks
./run-all-benchmarks.ps1

# Update baseline with new results
./run-all-benchmarks.ps1 -UpdateBaseline

# Custom parameters
./run-all-benchmarks.ps1 -MessageCount 1000000 -MessageSize 100 -BatchSize 1000

# Skip Docker-dependent tests
./run-all-benchmarks.ps1 -SkipKafka -SkipRedpanda

# Full verbose output
./run-all-benchmarks.ps1 -Verbose -UpdateBaseline
```

**Script Parameters**:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-MessageCount` | 1000000 | Number of messages |
| `-MessageSize` | 100 | Message size in bytes |
| `-BatchSize` | 1000 | Batch size |
| `-SkipKafka` | false | Skip Kafka comparison |
| `-SkipRedpanda` | false | Skip Redpanda comparison |
| `-UpdateBaseline` | false | Save results to benchmark-baseline.json |
| `-Verbose` | false | Show detailed output |

## Test Fixtures for xUnit

Surgewave provides test fixtures for easy integration:

```csharp
// Single test class
public class MyTests : IClassFixture<SurgewaveTestFixture>
{
    private readonly SurgewaveTestFixture _fixture;

    public MyTests(SurgewaveTestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task CanProduceAndConsume()
    {
        var bootstrap = _fixture.Surgewave.BootstrapServers;
        // ... test code
    }
}

// Shared across test collection
[CollectionDefinition("Surgewave")]
public class SurgewaveCollection : ICollectionFixture<SurgewaveCollectionFixture> { }

[Collection("Surgewave")]
public class MyTests
{
    private readonly SurgewaveCollectionFixture _fixture;

    public MyTests(SurgewaveCollectionFixture fixture)
    {
        _fixture = fixture;
    }
}
```

## Running Specific Test Categories

```bash
# By namespace
dotnet test --filter "Namespace~Kuestenlogik.Surgewave.Tests.Storage"
dotnet test --filter "Namespace~Kuestenlogik.Surgewave.Tests.Protocol"

# By trait (if defined)
dotnet test --filter "Category=Integration"
dotnet test --filter "Category=Unit"

# Exclude slow tests
dotnet test --filter "FullyQualifiedName!~Slow"
```

## CLI Health Diagnostics

Use the Surgewave CLI for health checks:

```bash
# Quick health check
surgewave health

# Full diagnostic report
surgewave health diagnose

# JSON output for monitoring
surgewave health diagnose --format json
```

Diagnostics include:
- TCP connectivity to broker
- Native Surgewave protocol health
- Kafka wire protocol compatibility
- Topic health and configuration audit
- Consumer group status
- Network latency analysis

## Docker-Based Testing

### Start Kafka for Comparison Tests

```bash
docker run -d --name kafka-test -p 29092:29092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:29092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:latest
```

### Start Redpanda for Comparison Tests

```bash
docker run -d --name redpanda-test -p 19092:19092 \
  redpandadata/redpanda:latest \
  redpanda start \
  --overprovisioned \
  --smp 1 \
  --memory 1G \
  --reserve-memory 0M \
  --node-id 0 \
  --check=false \
  --kafka-addr PLAINTEXT://0.0.0.0:19092 \
  --advertise-kafka-addr PLAINTEXT://localhost:19092
```

### Cleanup

```bash
docker stop kafka-test redpanda-test
docker rm kafka-test redpanda-test
```

## Continuous Integration

For CI pipelines, use self-contained tests that don't require external brokers:

```yaml
# GitHub Actions example
- name: Build
  run: dotnet build -c Release

- name: Run Tests
  run: dotnet test -c Release --no-build --verbosity minimal

- name: Run Embedded Benchmarks
  run: |
    cd benchmarks/Kuestenlogik.Surgewave.Benchmarks
    dotnet run -c Release --no-build -- embedded 100000 100 1000
```

## Troubleshooting

### Test fails with "No connection could be made"

**Solution**: Start the Surgewave broker before running integration tests:

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker
```

### Test times out

**Solution**:
1. Increase timeout in test
2. Check broker logs for errors
3. Verify port 9092 is not in use

### Messages not being consumed

**Solution**: Use `AutoOffsetReset.Earliest` in consumer config to read from the beginning:

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "test-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};
```

### Docker containers fail to start

**Solution**:
1. Verify Docker is running: `docker ps`
2. Check port availability: `netstat -an | grep 29092`
3. Check Docker logs: `docker logs kafka-test`

## Performance Expectations

On modern hardware (Intel Core i7, 16GB RAM):

| Protocol | Producer | Consumer |
|----------|----------|----------|
| Surgewave Native | 2M+ msg/sec | 2M+ msg/sec |
| Kafka Wire (Confluent.Kafka) | 1.5M+ msg/sec | 2M+ msg/sec |

See [COMPARISON.md](COMPARISON.md) for detailed benchmark results.

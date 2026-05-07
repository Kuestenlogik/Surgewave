# Surgewave Tests

Comprehensive test suite for Surgewave using xUnit.

## Test Projects

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Tests` | Legacy tests (being migrated) |
| `Kuestenlogik.Surgewave.Broker.Tests` | Broker component tests |
| `Kuestenlogik.Surgewave.Client.Tests` | Client library tests |
| `Kuestenlogik.Surgewave.Core.Tests` | Core models and utilities |
| `Kuestenlogik.Surgewave.Clustering.Tests` | Raft consensus and clustering |
| `Kuestenlogik.Surgewave.Protocol.Kafka.Tests` | Kafka protocol parsing |
| `Kuestenlogik.Surgewave.Protocol.Native.Tests` | Native protocol tests |
| `Kuestenlogik.Surgewave.Storage.Engine.Tests` | Storage engine abstraction |
| `Kuestenlogik.Surgewave.Storage.Engine.Arrow.Tests` | Arrow storage backend |
| `Kuestenlogik.Surgewave.Storage.Tiering.Tests` | Tiered storage (S3, Azure, GCP) |
| `Kuestenlogik.Surgewave.Schema.Registry.Tests` | Schema Registry |
| `Kuestenlogik.Surgewave.Streams.Tests` | Stream processing |
| `Kuestenlogik.Surgewave.Runtime.Tests` | Embedded SurgewaveRuntime |
| `Kuestenlogik.Surgewave.Connect.Http.Tests` | HTTP connector |
| `Kuestenlogik.Surgewave.Api.Grpc.Tests` | gRPC API |
| `Kuestenlogik.Surgewave.IntegrationTests` | Full integration tests |
| `Kuestenlogik.Surgewave.Testing` | Shared test utilities |

## Running Tests

```bash
# Run all tests
dotnet test

# Run specific project
dotnet test tests/Kuestenlogik.Surgewave.Broker.Tests

# Run with verbose output
dotnet test --logger "console;verbosity=detailed"

# Run specific test
dotnet test --filter "FullyQualifiedName~MyTestClass"

# Run with coverage
dotnet test --collect:"XPlat Code Coverage"
```

## Test Categories

Tests are organized by functionality:
- Unit tests - Fast, isolated tests
- Integration tests - Require broker or containers
- Performance tests - Timing-sensitive tests

## Test Configuration

Tests use `xunit.runner.json` for configuration:
```json
{
  "parallelizeTestCollections": true,
  "maxParallelThreads": -1
}
```

## Adding Tests

1. Create test class with `[Fact]` or `[Theory]` attributes
2. Use `Kuestenlogik.Surgewave.Testing` for shared fixtures
3. Follow naming: `MethodName_Scenario_ExpectedResult`

Example:
```csharp
public class MyComponentTests
{
    [Fact]
    public void Process_ValidInput_ReturnsSuccess()
    {
        // Arrange
        var component = new MyComponent();

        // Act
        var result = component.Process("input");

        // Assert
        Assert.True(result.IsSuccess);
    }
}
```

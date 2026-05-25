using Testcontainers.Redpanda;
using Xunit;

namespace Kuestenlogik.Surgewave.Tests.Fixtures;

/// <summary>
/// Docker fixture for Redpanda (Kafka-compatible broker) using Testcontainers.NET.
/// Redpanda is faster to start than Kafka and doesn't need ZooKeeper.
/// </summary>
public sealed class RedpandaContainerFixture : IAsyncLifetime
{
    private readonly RedpandaContainer _container;

    /// <summary>
    /// The bootstrap servers string for connecting to the Redpanda broker.
    /// </summary>
    public string BootstrapServers => _container.GetBootstrapAddress();

    /// <summary>
    /// The Schema Registry URL for Redpanda's built-in schema registry.
    /// </summary>
    public string SchemaRegistryAddress => _container.GetSchemaRegistryAddress();

    /// <summary>
    /// Whether the container is currently running.
    /// </summary>
    public bool IsRunning { get; private set; }

    public RedpandaContainerFixture()
    {
        _container = new RedpandaBuilder()
            .WithImage("redpandadata/redpanda:v24.1.1")
            .Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        IsRunning = true;
    }

    public async ValueTask DisposeAsync()
    {
        IsRunning = false;
        await _container.DisposeAsync();
    }
}

/// <summary>
/// Collection definition for sharing the Redpanda container across multiple test classes.
/// Tests in this collection run sequentially to avoid port conflicts.
/// </summary>
[CollectionDefinition("Redpanda")]
public class RedpandaCollection : ICollectionFixture<RedpandaContainerFixture>
{
}

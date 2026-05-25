using Testcontainers.Kafka;
using Xunit;

namespace Kuestenlogik.Surgewave.Tests.Fixtures;

/// <summary>
/// Docker fixture for Apache Kafka using Testcontainers.NET.
/// Uses Confluent's Kafka image with KRaft mode (no ZooKeeper).
/// </summary>
public sealed class KafkaContainerFixture : IAsyncLifetime
{
    private readonly KafkaContainer _container;

    /// <summary>
    /// The bootstrap servers string for connecting to the Kafka broker.
    /// </summary>
    public string BootstrapServers => _container.GetBootstrapAddress();

    /// <summary>
    /// Whether the container is currently running.
    /// </summary>
    public bool IsRunning { get; private set; }

    public KafkaContainerFixture()
    {
        _container = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.6.0")
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
/// Collection definition for sharing the Kafka container across multiple test classes.
/// Tests in this collection run sequentially to avoid port conflicts.
/// </summary>
[CollectionDefinition("Kafka")]
public class KafkaCollection : ICollectionFixture<KafkaContainerFixture>
{
}

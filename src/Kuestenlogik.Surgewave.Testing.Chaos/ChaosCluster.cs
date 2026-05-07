using Confluent.Kafka;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// A multi-broker chaos cluster for resilience testing.
/// Each broker has its own <see cref="ChaosEngine"/> for independent fault injection.
/// Provides convenience methods for partitioning, crashing, and injecting latency into individual brokers.
/// </summary>
public sealed class ChaosCluster : IAsyncDisposable
{
    private readonly Dictionary<int, (SurgewaveRuntime Runtime, ChaosEngine Engine)> _brokers = new();
    private readonly ILoggerFactory _loggerFactory;
    private bool _disposed;

    private ChaosCluster(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    /// <summary>
    /// Creates a chaos cluster with the specified number of brokers.
    /// Each broker runs with in-memory storage wrapped in chaos fault injection.
    /// </summary>
    /// <param name="brokerCount">Number of brokers to start.</param>
    /// <param name="loggerFactory">Optional logger factory for diagnostics.</param>
    /// <returns>A running chaos cluster.</returns>
    public static async Task<ChaosCluster> CreateAsync(int brokerCount, ILoggerFactory? loggerFactory = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(brokerCount, 1);

        var factory = loggerFactory ?? NullLoggerFactory.Instance;
        var cluster = new ChaosCluster(factory);

        try
        {
            for (int i = 0; i < brokerCount; i++)
            {
                var engine = new ChaosEngine();
                var runtime = await SurgewaveRuntime.CreateBuilder()
                    .WithBrokerId(i)
                    .WithPort(0)
                    .WithStorageEngine(StorageEngines.Memory)
                    .WithChaosEngine(engine, factory)
                    .WithAutoCreateTopics(true)
                    .WithCleanup(true)
                    .WithLogging(factory)
                    .Build()
                    .StartAsync();

                cluster._brokers[i] = (runtime, engine);
            }

            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Gets the chaos engine for a specific broker.
    /// </summary>
    /// <param name="brokerId">The broker ID.</param>
    /// <returns>The chaos engine for the broker.</returns>
    public ChaosEngine GetEngine(int brokerId)
    {
        if (!_brokers.TryGetValue(brokerId, out var entry))
            throw new ArgumentException($"Broker {brokerId} not found in cluster", nameof(brokerId));

        return entry.Engine;
    }

    /// <summary>
    /// Gets the Surgewave runtime for a specific broker.
    /// </summary>
    /// <param name="brokerId">The broker ID.</param>
    /// <returns>The Surgewave runtime for the broker.</returns>
    public SurgewaveRuntime GetRuntime(int brokerId)
    {
        if (!_brokers.TryGetValue(brokerId, out var entry))
            throw new ArgumentException($"Broker {brokerId} not found in cluster", nameof(brokerId));

        return entry.Runtime;
    }

    /// <summary>
    /// Gets the bootstrap servers string for connecting a Kafka client to a specific broker.
    /// </summary>
    /// <param name="brokerId">The broker ID.</param>
    /// <returns>The bootstrap servers string (host:port).</returns>
    public string GetBootstrapServers(int brokerId)
    {
        return GetRuntime(brokerId).BootstrapServers;
    }

    /// <summary>
    /// Gets the IDs of all brokers in the cluster.
    /// </summary>
    public IReadOnlyList<int> BrokerIds => _brokers.Keys.OrderBy(id => id).ToList();

    /// <summary>
    /// Partitions a broker from all other brokers in the cluster.
    /// </summary>
    /// <param name="brokerId">The broker ID to isolate.</param>
    /// <returns>A scenario that can be healed or disposed.</returns>
    public NetworkPartitionScenario PartitionBroker(int brokerId)
    {
        var engine = GetEngine(brokerId);
        var otherIds = _brokers.Keys.Where(id => id != brokerId);
        return NetworkPartitionScenario.Create(engine, brokerId, otherIds);
    }

    /// <summary>
    /// Simulates a crash on the specified broker.
    /// </summary>
    /// <param name="brokerId">The broker ID to crash.</param>
    /// <returns>A scenario that can be recovered or disposed.</returns>
    public BrokerCrashScenario CrashBroker(int brokerId)
    {
        var engine = GetEngine(brokerId);
        return BrokerCrashScenario.Create(engine, brokerId);
    }

    /// <summary>
    /// Injects latency into all operations on the specified broker.
    /// </summary>
    /// <param name="brokerId">The broker ID to slow down.</param>
    /// <param name="latency">The amount of latency to inject.</param>
    public void InjectLatency(int brokerId, TimeSpan latency)
    {
        var engine = GetEngine(brokerId);
        engine.ActivateFault(FaultType.SlowNetwork, new FaultScope { BrokerId = brokerId }, latency);
    }

    /// <summary>
    /// Deactivates all faults on all brokers, restoring normal operation.
    /// </summary>
    public void HealAll()
    {
        foreach (var (_, engine) in _brokers.Values)
        {
            engine.DeactivateAll();
        }
    }

    /// <summary>
    /// Produces messages to a topic on the specified broker (defaults to broker 0).
    /// </summary>
    /// <param name="topic">The topic to produce to.</param>
    /// <param name="messageCount">Number of messages to produce.</param>
    /// <param name="brokerId">The broker ID to produce through. Defaults to 0.</param>
    public async Task ProduceAsync(string topic, int messageCount, int brokerId = 0)
    {
        var bootstrapServers = GetBootstrapServers(brokerId);
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = $"chaos-producer-{Guid.NewGuid():N}",
            Acks = Acks.Leader,
            MessageTimeoutMs = 5000
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"key-{i}",
                Value = $"chaos-message-{i}"
            });
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// Consumes messages from a topic on the specified broker (defaults to broker 0).
    /// </summary>
    /// <param name="topic">The topic to consume from.</param>
    /// <param name="expectedCount">Number of messages to wait for.</param>
    /// <param name="timeout">Maximum time to wait.</param>
    /// <param name="brokerId">The broker ID to consume from. Defaults to 0.</param>
    /// <returns>The consumed message values.</returns>
    public async Task<List<string>> ConsumeAsync(string topic, int expectedCount, TimeSpan timeout, int brokerId = 0)
    {
        var bootstrapServers = GetBootstrapServers(brokerId);
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"chaos-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var messages = new List<string>();

        await Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(topic);

            var deadline = DateTime.UtcNow + timeout;
            while (messages.Count < expectedCount && DateTime.UtcNow < deadline)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result?.Message?.Value != null)
                {
                    messages.Add(result.Message.Value);
                }
            }

            consumer.Close();
        });

        return messages;
    }

    /// <summary>
    /// Creates a topic on the specified broker using the LogManager directly.
    /// </summary>
    /// <param name="topic">Topic name to create.</param>
    /// <param name="partitions">Number of partitions.</param>
    /// <param name="replicationFactor">Replication factor.</param>
    /// <param name="brokerId">The broker ID to create the topic on. Defaults to 0.</param>
    public async Task CreateTopicAsync(string topic, int partitions = 1, int replicationFactor = 1, int brokerId = 0)
    {
        var runtime = GetRuntime(brokerId);
        await runtime.LogManager.CreateTopicAsync(topic, partitions, (short)replicationFactor);
    }

    /// <summary>
    /// Disposes all broker runtimes in the cluster.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var (runtime, engine) in _brokers.Values)
        {
            engine.DeactivateAll();
            try
            {
                await runtime.DisposeAsync();
            }
            catch
            {
                // Ignore disposal errors during cleanup
            }
        }

        _brokers.Clear();
    }
}

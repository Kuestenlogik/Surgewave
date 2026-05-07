using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing.Chaos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld;

/// <summary>
/// Creates and manages an embedded multi-broker Surgewave cluster for benchmark scenarios.
/// Each broker runs in-process with the specified storage engine.
/// Provides convenience methods for obtaining clients and chaos engines.
/// </summary>
public sealed class ClusterSetup : IAsyncDisposable
{
    private readonly List<SurgewaveRuntime> _runtimes = [];
    private readonly Dictionary<int, ChaosEngine> _chaosEngines = [];
    private bool _disposed;

    /// <summary>All broker runtimes in the cluster.</summary>
    public IReadOnlyList<SurgewaveRuntime> Runtimes => _runtimes;

    /// <summary>Number of brokers in the cluster.</summary>
    public int BrokerCount => _runtimes.Count;

    /// <summary>
    /// Creates an embedded multi-broker cluster with the specified storage engine.
    /// </summary>
    /// <param name="brokerCount">Number of brokers to start.</param>
    /// <param name="storageEngine">Storage engine name to use (default: "memory").</param>
    /// <param name="partitions">Default partition count for auto-created topics.</param>
    /// <param name="enableChaos">Whether to enable chaos engines on each broker.</param>
    /// <returns>A running cluster setup.</returns>
    public static async Task<ClusterSetup> CreateAsync(
        int brokerCount,
        string storageEngine = StorageEngines.Memory,
        int partitions = 1,
        bool enableChaos = false)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(brokerCount, 1);

        var cluster = new ClusterSetup();

        try
        {
            for (int i = 0; i < brokerCount; i++)
            {
                var builder = SurgewaveRuntime.CreateBuilder()
                    .WithBrokerId(i)
                    .WithPort(0)
                    .WithStorageEngine(storageEngine)
                    .WithAutoCreateTopics(true)
                    .WithPartitions(partitions)
                    .WithCleanup(true)
                    .WithLogging(NullLoggerFactory.Instance);

                if (enableChaos)
                {
                    var engine = new ChaosEngine();
                    cluster._chaosEngines[i] = engine;
                    builder.WithChaosEngine(engine);
                }

                var runtime = await builder.Build().StartAsync();
                cluster._runtimes.Add(runtime);
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
    /// Gets a connected native client for the specified broker.
    /// Caller owns the client and must dispose it.
    /// </summary>
    /// <param name="brokerId">Broker index (0-based).</param>
    /// <returns>A connected SurgewaveNativeClient.</returns>
    public async Task<SurgewaveNativeClient> GetClientAsync(int brokerId = 0)
    {
        var runtime = _runtimes[brokerId];
        var client = new SurgewaveNativeClient("localhost", runtime.Port);
        await client.ConnectAsync();
        return client;
    }

    /// <summary>
    /// Gets the bootstrap servers string for connecting via Kafka protocol.
    /// </summary>
    /// <param name="brokerId">Broker index (0-based).</param>
    public string GetBootstrapServers(int brokerId = 0)
    {
        return _runtimes[brokerId].BootstrapServers;
    }

    /// <summary>
    /// Gets the port for the specified broker.
    /// </summary>
    /// <param name="brokerId">Broker index (0-based).</param>
    public int GetPort(int brokerId = 0)
    {
        return _runtimes[brokerId].Port;
    }

    /// <summary>
    /// Gets the chaos engine for the specified broker (only available when enableChaos=true).
    /// </summary>
    /// <param name="brokerId">Broker index (0-based).</param>
    public ChaosEngine GetChaosEngine(int brokerId)
    {
        if (!_chaosEngines.TryGetValue(brokerId, out var engine))
            throw new InvalidOperationException($"Chaos engine not available for broker {brokerId}. Create cluster with enableChaos=true.");

        return engine;
    }

    /// <summary>
    /// Creates a topic on the first broker.
    /// </summary>
    public async Task CreateTopicAsync(string topic, int partitions = 1)
    {
        await using var client = await GetClientAsync(0);
        await client.Topics.CreateAsync(topic, partitions);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var engine in _chaosEngines.Values)
        {
            engine.DeactivateAll();
        }

        foreach (var runtime in _runtimes)
        {
            try
            {
                await runtime.DisposeAsync();
            }
            catch
            {
                // Ignore disposal errors during cleanup
            }
        }

        _runtimes.Clear();
        _chaosEngines.Clear();
    }
}

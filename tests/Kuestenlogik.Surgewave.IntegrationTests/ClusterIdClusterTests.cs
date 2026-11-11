using System.Net;
using System.Net.Sockets;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// A cluster has one id (#203): the first controller writes it to the metadata log, every broker adopts and persists
/// it, and every broker tells a client that id — the same one, before and after a restart. Each broker generated and
/// persisted its own id before, so the brokers told clients a fixed default instead of the cluster's id.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class ClusterIdClusterTests(ITestOutputHelper output) : IAsyncLifetime
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-cluster-id-it-" + Guid.NewGuid().ToString("N"));
    private readonly ILoggerFactory _loggerFactory = LoggerFactory.Create(builder =>
    {
        builder.AddProvider(new XunitLoggerProvider(output));
        builder.SetMinimumLevel(LogLevel.Warning);
    });
    private readonly SurgewaveRuntime?[] _brokers = new SurgewaveRuntime?[3];
    private int[] _ports = [];
    private string _quorum = "";
    private string[] _nodes = [];

    public async ValueTask InitializeAsync()
    {
        // A declared quorum needs every address before any broker starts, so the ports are reserved up front; a
        // restarted broker comes back on its own.
        _ports = ReservePorts(6);
        _quorum = string.Join(",", Enumerable.Range(0, 3).Select(i => $"{i + 1}@127.0.0.1:{_ports[3 + i]}"));
        _nodes = [.. Enumerable.Range(0, 3).Select(i => $"{i + 1}:127.0.0.1:{_ports[i]}:{_ports[3 + i]}")];

        for (var i = 0; i < 3; i++)
            _brokers[i] = await StartBrokerAsync(i);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var broker in _brokers)
        {
            if (broker is null) continue;
            try { await broker.DisposeAsync(); } catch (Exception ex) { output.WriteLine($"dispose: {ex.Message}"); }
        }
        _loggerFactory.Dispose();
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact(Timeout = 180_000)]
    public async Task EveryBrokerTellsAClientTheOneClusterIdItPersistedAndARestartKeepsIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await AwaitEveryBrokerHasAppliedATopicAsync(_brokers.Select(b => b!), ct);

        var reported = new List<string>();
        foreach (var broker in _brokers.Select(b => b!))
        {
            var clusterId = await DescribeClusterIdAsync(broker, ct);
            output.WriteLine($"broker {broker.BrokerId} reports cluster id {clusterId}");
            Assert.Equal(PersistedClusterId(broker), clusterId);
            reported.Add(clusterId);
        }
        var oneId = Assert.Single(reported.Distinct());

        // A broker that restarts tells a client the same id: it persisted it.
        var restarting = _brokers.Select(b => b!).First(b => !b.IsController);
        var index = Array.IndexOf(_brokers, restarting);
        await restarting.DisposeAsync();
        _brokers[index] = null;
        _brokers[index] = await StartBrokerAsync(index);
        await AwaitEveryBrokerHasAppliedATopicAsync([_brokers[index]!], ct);

        Assert.Equal(oneId, await DescribeClusterIdAsync(_brokers[index]!, ct));
        Assert.Equal(oneId, PersistedClusterId(_brokers[index]!));
    }

    [Fact(Timeout = 180_000)]
    public async Task ABrokerThatPersistedAnotherClustersIdIsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        var clusterId = await _brokers[0]!.ClusterState!.ClusterIdCommitted.WaitAsync(Timeout, ct);
        foreach (var broker in _brokers.Select(b => b!))
            Assert.Equal(clusterId, await broker.ClusterIdentity.Known.WaitAsync(Timeout, ct));

        // A broker that is not the controller comes back carrying another cluster's id, as if its data directory
        // belonged elsewhere.
        var stranger = Array.FindIndex(_brokers, b => !b!.IsController);
        await _brokers[stranger]!.DisposeAsync();
        _brokers[stranger] = null;
        await File.WriteAllTextAsync(Path.Combine(DataDirectory(stranger), "cluster.id"), "another-cluster", ct);
        _brokers[stranger] = await StartBrokerAsync(stranger);

        // The controller refuses its registration with INCONSISTENT_CLUSTER_ID, and it keeps its own id.
        Assert.Equal("another-cluster", await _brokers[stranger]!.ClusterIdentity.Refused.WaitAsync(Timeout, ct));
        Assert.Equal("another-cluster", PersistedClusterId(_brokers[stranger]!));

        // A configured id that is not the persisted one stops a broker before it starts.
        var reconfigured = Array.FindIndex(_brokers, b => b is not null && !b.IsController && !ReferenceEquals(b, _brokers[stranger]));
        await _brokers[reconfigured]!.DisposeAsync();
        _brokers[reconfigured] = null;
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => StartBrokerAsync(reconfigured, configuredClusterId: "configured-elsewhere"));
        Assert.Contains(clusterId, ex.Message, StringComparison.Ordinal);
    }

    private string DataDirectory(int index) => Path.Combine(_root, $"broker-{index + 1}");

    private Task<SurgewaveRuntime> StartBrokerAsync(int index, string? configuredClusterId = null) =>
        SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(index + 1)
            .WithHost("127.0.0.1")
            .WithPort(_ports[index])
            .WithReplicationPort(_ports[3 + index])
            .WithCluster([.. _nodes.Where((_, n) => n != index)])
            .WithControllerQuorum(_quorum)
            .WithClusterId(configuredClusterId)
            .WithOffsetsTopicPartitions(1)
            .WithTransactionStateLogPartitions(1)
            .WithShareStateTopicPartitions(1)
            .WithStorageEngine(StorageEngines.Memory)
            .WithDataDirectory(DataDirectory(index))
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(10)
            .Build()
            .StartAsync();

    // A topic created now is in the metadata log after the cluster id: a broker that has applied it has applied the
    // id. Waited for on each broker's topic signal, taken before looking.
    private async Task AwaitEveryBrokerHasAppliedATopicAsync(IEnumerable<SurgewaveRuntime> brokers, CancellationToken ct)
    {
        var topic = $"cluster-id-{Guid.NewGuid():N}";
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap() }).Build())
        {
            await admin.CreateTopicsAsync(
                [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
                new CreateTopicsOptions { OperationTimeout = Timeout, RequestTimeout = Timeout });
        }

        foreach (var broker in brokers)
        {
            while (true)
            {
                var changed = broker.ClusterState!.TopicsChanged;
                if (broker.ClusterState.GetTopic(topic) is not null)
                    break;
                await changed.WaitAsync(Timeout, ct);
            }
        }
    }

    private string Bootstrap() => string.Join(",", _brokers.Where(b => b is not null).Select(b => b!.BootstrapServers));

    // The id a Confluent client is told by this broker: bootstrapped to it alone.
    private static async Task<string> DescribeClusterIdAsync(SurgewaveRuntime broker, CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker.BootstrapServers }).Build();
        var described = await admin.DescribeClusterAsync(new DescribeClusterOptions { RequestTimeout = Timeout }).WaitAsync(ct);
        return described.ClusterId;
    }

    private static string? PersistedClusterId(SurgewaveRuntime broker)
    {
        var path = Path.Combine(broker.DataDirectory, "cluster.id");
        return File.Exists(path) ? File.ReadAllText(path) : null;
    }

    private static int[] ReservePorts(int count)
    {
        var listeners = new List<TcpListener>(count);
        try
        {
            for (var i = 0; i < count; i++)
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                listeners.Add(listener);
            }
            return [.. listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port)];
        }
        finally
        {
            foreach (var listener in listeners) listener.Stop();
        }
    }
}

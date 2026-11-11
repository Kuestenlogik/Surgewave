using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests.Fixtures;

/// <summary>
/// Three brokers with a declared quorum, started once every broker knows <c>__consumer_offsets</c> and
/// <c>__transaction_state</c> with a leader for each partition: the coordinators of #204, #205 and #197 can answer.
/// Both topics get few partitions, so a test cluster replicates little.
/// </summary>
public sealed class CoordinatorCluster : IAsyncDisposable
{
    public const int CoordinatorPartitions = 3;

    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;
    private readonly HashSet<SurgewaveRuntime> _stopped = [];
    private readonly string _host;
    private readonly Func<SurgewaveRuntimeBuilder, int, SurgewaveRuntimeBuilder>? _configure;

    private CoordinatorCluster(ITestOutputHelper output, string host, Func<SurgewaveRuntimeBuilder, int, SurgewaveRuntimeBuilder>? configure)
    {
        _output = output;
        _host = host;
        _configure = configure;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(new XunitLoggerProvider(output));
            builder.SetMinimumLevel(LogLevel.Warning);
        });
    }

    /// <summary>The brokers, ids 1 to 3.</summary>
    public List<SurgewaveRuntime> Brokers { get; } = [];

    /// <summary>The brokers still running.</summary>
    public IEnumerable<SurgewaveRuntime> Running => Brokers.Where(b => !_stopped.Contains(b));

    /// <param name="host">
    /// The address every broker binds and advertises: loopback by default, the host's outbound address for a client in
    /// a container (<see cref="ContainerReachableHost"/>).
    /// </param>
    /// <param name="configure">Applied to each broker's builder last, with the broker's id — a test's own storage, say.</param>
    public static async Task<CoordinatorCluster> StartAsync(
        ITestOutputHelper output,
        string host = "127.0.0.1",
        Func<SurgewaveRuntimeBuilder, int, SurgewaveRuntimeBuilder>? configure = null)
    {
        var cluster = new CoordinatorCluster(output, host, configure);
        try
        {
            await cluster.StartBrokersAsync();
            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    /// <summary>The broker that coordinates <paramref name="key"/>, as the running brokers see it.</summary>
    public SurgewaveRuntime CoordinatorOf(CoordinatorKind kind, string key)
    {
        var id = new ClusterCoordinatorLocator(Running.First().ClusterState!).Locate(kind, key).BrokerId;
        return Brokers.Single(b => b.BrokerId == id);
    }

    /// <summary>Stops a broker; the cluster takes over its partitions.</summary>
    public async Task StopAsync(SurgewaveRuntime broker)
    {
        _stopped.Add(broker);
        await broker.DisposeAsync();
        _output.WriteLine($"broker {broker.BrokerId} stopped");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var broker in Running.ToList())
        {
            try { await broker.DisposeAsync(); } catch (Exception ex) { _output.WriteLine($"dispose: {ex.Message}"); }
        }
        _loggerFactory.Dispose();
    }

    private async Task StartBrokersAsync()
    {
        // A declared quorum needs every address before any broker starts, so the ports are reserved up front.
        var ports = ReservePorts(6, System.Net.IPAddress.Parse(_host));
        var quorum = string.Join(",", Enumerable.Range(0, 3).Select(i => $"{i + 1}@{_host}:{ports[3 + i]}"));
        var nodes = Enumerable.Range(0, 3).Select(i => $"{i + 1}:{_host}:{ports[i]}:{ports[3 + i]}").ToArray();

        for (var i = 0; i < 3; i++)
        {
            var self = i;
            var builder = SurgewaveRuntime.CreateBuilder()
                .WithBrokerId(i + 1)
                .WithHost(_host)
                .WithPort(ports[i])
                .WithReplicationPort(ports[3 + i])
                .WithCluster([.. nodes.Where((_, n) => n != self)])
                .WithControllerQuorum(quorum)
                .WithOffsetsTopicPartitions(CoordinatorPartitions)
                .WithTransactionStateLogPartitions(CoordinatorPartitions)
                .WithShareStateTopicPartitions(CoordinatorPartitions)
                .WithStorageEngine(StorageEngines.Memory)
                .WithLogging(_loggerFactory)
                .WithShutdownTimeout(10);
            Brokers.Add(await (_configure?.Invoke(builder, i + 1) ?? builder)
                .Build()
                .StartAsync());
        }

        foreach (var self in Brokers)
        {
            foreach (var peer in Brokers.Where(p => !ReferenceEquals(p, self)))
            {
                self.ClusterState!.AddBroker(new BrokerNode
                {
                    BrokerId = peer.BrokerId,
                    Host = peer.Host,
                    Port = peer.Port,
                    ReplicationPort = peer.ReplicationPort,
                });
            }
        }

        Assert.True(
            await TestWaitHelpers.WaitForClusterStabilizationAsync(Brokers, output: _output),
            "the three-broker cluster did not stabilise");

        // Every broker has to know the coordinator topics and a leader for each partition.
        foreach (var topic in new[] { CoordinatorPartitioner.GroupTopic, CoordinatorPartitioner.TransactionTopic, CoordinatorPartitioner.ShareStateTopic })
        {
            Assert.True(
                await TestWaitHelpers.WaitForConditionAsync(
                    () => Brokers.All(b => Enumerable.Range(0, CoordinatorPartitions).All(p => LeaderOf(b, topic, p) > 0)),
                    timeout: TimeSpan.FromSeconds(60), output: _output),
                $"{topic} did not get a leader for every partition on every broker: " + string.Join("; ",
                    Brokers.Select(b => $"broker {b.BrokerId}{(b.IsController ? " (controller)" : "")}: " + string.Join(",",
                        Enumerable.Range(0, CoordinatorPartitions).Select(p => $"{p}->{LeaderOf(b, topic, p)?.ToString() ?? "unknown"}")))));
        }
    }

    // The leader a broker knows for a partition, null while it does not know the partition.
    public static int? LeaderOf(SurgewaveRuntime broker, string topic, int partition) =>
        broker.ClusterState!.GetPartitionState(new Kuestenlogik.Surgewave.Core.Models.TopicPartition
        {
            Topic = topic,
            Partition = partition,
        })?.LeaderBrokerId;

    private static int[] ReservePorts(int count, System.Net.IPAddress address)
    {
        var listeners = new List<System.Net.Sockets.TcpListener>(count);
        try
        {
            for (var i = 0; i < count; i++)
            {
                var listener = new System.Net.Sockets.TcpListener(address, 0);
                listener.Start();
                listeners.Add(listener);
            }
            return [.. listeners.Select(l => ((System.Net.IPEndPoint)l.LocalEndpoint).Port)];
        }
        finally
        {
            foreach (var listener in listeners) listener.Stop();
        }
    }
}

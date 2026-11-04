using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A partition leader asks a transactional id's coordinator on another broker about a partition of the transaction
/// (#197) — over a real TCP loopback to the coordinator's inter-broker server, which answers with its own
/// coordinator's verdict. The leader used to ask only its own coordinator, which answered NOT_COORDINATOR.
/// </summary>
public sealed class NativeTransactionVerifierTests : IAsyncLifetime
{
    private const int TransactionStatePartitions = 3;

    private readonly CancellationTokenSource _cts = new(TimeSpan.FromSeconds(20));

    // The coordinator (broker 2) behind a loopback listener.
    private readonly ClusterState _coordinatorState = new();
    private ReplicaManager _coordinatorReplicas = null!;
    private readonly RecordingCoordinator _coordinator = new();
    private Kuestenlogik.Surgewave.Transport.IPeerListener _listener = null!;
    private Task? _serverTask;

    // The partition leader (broker 1).
    private readonly ClusterState _leaderState = new() { LocalBrokerId = 1 };
    private ConnectionPool _pool = null!;
    private NativeTransactionVerifier _verifier = null!;

    public async ValueTask InitializeAsync()
    {
        var transport = new TcpPeerTransport();
        var config = new ClusteringConfig { BrokerId = 2, Host = "localhost", Port = 9094, RebalanceCheckIntervalSeconds = 5 };
        var logs = new LogManager(Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"), new MemoryLogSegmentFactory());
        _coordinatorReplicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, _coordinatorState, logs, config, transport);
        var server = new NativeInterBrokerServer(
            NullLogger<NativeInterBrokerServer>.Instance,
            new ClusterStateInterBrokerService(
                NullLogger<ClusterStateInterBrokerService>.Instance,
                _coordinatorState, _coordinatorReplicas, logs, localBrokerId: 2, transactionVerifier: _coordinator));

        _listener = transport.CreateListener(new IPEndPoint(IPAddress.Loopback, 0));
        await _listener.StartAsync();
        _serverTask = Task.Run(async () =>
        {
            await using var conn = await _listener.AcceptAsync(_cts.Token);
            await using var lease = await conn.AcceptInboundStreamAsync(_cts.Token);
            while (await server.HandleSingleAsync(lease.Stream, _cts.Token)) { }
        }, _cts.Token);

        _leaderState.AddBroker(new BrokerNode { BrokerId = 1, Host = "localhost", Port = 9093 });
        _leaderState.AddBroker(new BrokerNode { BrokerId = 2, Host = "127.0.0.1", Port = 9094, ReplicationPort = _listener.LocalEndPoint.Port });
        _leaderState.AddTopic(new TopicMetadata
        {
            Name = CoordinatorPartitioner.TransactionTopic,
            TopicId = Guid.NewGuid(),
            PartitionCount = TransactionStatePartitions,
            ReplicationFactor = 2,
            Config = [],
            CreatedAt = DateTime.UtcNow,
        });
        for (var p = 0; p < TransactionStatePartitions; p++)
            _leaderState.AssignReplicas(new TopicPartition { Topic = CoordinatorPartitioner.TransactionTopic, Partition = p }, [2, 1]);

        _pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, transport);
        _verifier = new NativeTransactionVerifier(_pool, _leaderState, localBrokerId: 1, NullLogger<NativeTransactionVerifier>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _pool.Dispose();
        try { await (_serverTask ?? Task.CompletedTask); }
        catch (OperationCanceledException) { /* server loop ended by test teardown */ }
        await _listener.DisposeAsync();
        await _coordinatorReplicas.DisposeAsync();
        _cts.Dispose();
    }

    [Fact]
    public async Task TheCoordinatorOnAnotherBrokerIsAskedAndItsAnswerReturned()
    {
        _coordinator.Answer = TxnErrorStatus.TransactionAbortable;
        var partition = new TopicPartition { Topic = "orders", Partition = 4 };

        var status = await _verifier.VerifyPartitionAsync("tx-1", producerId: 42, producerEpoch: 3, partition, TransactionVersion.Tv2, _cts.Token);

        Assert.Equal(TxnErrorStatus.TransactionAbortable, status);
        Assert.Equal(("tx-1", 42L, (short)3, partition, TransactionVersion.Tv2), Assert.Single(_coordinator.Asked));
    }

    [Fact]
    public async Task NoLeaderForTheIdsPartitionIsRetriable()
    {
        for (var p = 0; p < TransactionStatePartitions; p++)
            _leaderState.UpdatePartitionState(new TopicPartition { Topic = CoordinatorPartitioner.TransactionTopic, Partition = p }, s => s.LeaderBrokerId = -1);

        var status = await _verifier.VerifyPartitionAsync("tx-1", 42, 3, new TopicPartition { Topic = "orders", Partition = 0 }, TransactionVersion.Tv1, _cts.Token);

        Assert.Equal(TxnErrorStatus.CoordinatorNotAvailable, status);
        Assert.Empty(_coordinator.Asked);
    }

    // The coordinator's own answer, as the broker's TransactionCoordinator gives it.
    private sealed class RecordingCoordinator : ITransactionPartitionVerifier
    {
        public TxnErrorStatus Answer { get; set; }

        public List<(string, long, short, TopicPartition, TransactionVersion)> Asked { get; } = [];

        public Task<TxnErrorStatus> VerifyPartitionAsync(
            string transactionalId, long producerId, short producerEpoch, TopicPartition partition, TransactionVersion version,
            CancellationToken cancellationToken)
        {
            Asked.Add((transactionalId, producerId, producerEpoch, partition, version));
            return Task.FromResult(Answer);
        }
    }
}

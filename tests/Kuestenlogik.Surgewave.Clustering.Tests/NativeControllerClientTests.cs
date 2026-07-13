using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc5 — end-to-end coverage for the native controller→replica client: frames sent through the
/// pooled peer transport to a real TCP loopback listener, received by the
/// <see cref="NativeInterBrokerServer"/>, applied by <see cref="ClusterStateInterBrokerService"/>,
/// and acked back. This is the full native control-plane path both directions (controller push and
/// reverse ISR report), minus only the ReplicationServer port multiplex (covered by Inc4).
/// </summary>
public class NativeControllerClientTests : IAsyncLifetime
{
    private readonly CancellationTokenSource _cts = new(TimeSpan.FromSeconds(20));

    // Receiver (broker 2) — full native receive stack behind a TCP loopback listener.
    private readonly ClusterState _receiverState = new();
    private ReplicaManager _receiverReplicas = null!;
    private readonly NativeInterBrokerServerTests.StubIsrApplier _receiverIsrApplier = new();
    private Kuestenlogik.Surgewave.Transport.IPeerListener _listener = null!;
    private Task? _serverTask;

    // Sender (broker 1, the controller).
    private readonly ClusterState _senderState = new();
    private ConnectionPool _pool = null!;
    private NativeControllerClient _client = null!;

    public async ValueTask InitializeAsync()
    {
        var transport = new TcpPeerTransport();

        var receiverConfig = new ClusteringConfig { BrokerId = 2, Host = "localhost", Port = 9094, RebalanceCheckIntervalSeconds = 5 };
        var receiverLogs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        _receiverReplicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, _receiverState, receiverLogs, receiverConfig, transport);
        var server = new NativeInterBrokerServer(
            NullLogger<NativeInterBrokerServer>.Instance,
            new ClusterStateInterBrokerService(
                NullLogger<ClusterStateInterBrokerService>.Instance,
                _receiverState, _receiverReplicas, receiverLogs, localBrokerId: 2, _receiverIsrApplier));

        _listener = transport.CreateListener(new IPEndPoint(IPAddress.Loopback, 0));
        await _listener.StartAsync();

        // One connection, one stream, many sequential RPCs — matches how the pooled client reuses
        // its connection across sends.
        _serverTask = Task.Run(async () =>
        {
            await using var conn = await _listener.AcceptAsync(_cts.Token);
            await using var lease = await conn.AcceptInboundStreamAsync(_cts.Token);
            while (await server.HandleSingleAsync(lease.Stream, _cts.Token)) { }
        }, _cts.Token);

        // Sender: broker 1 is the controller at epoch 7; broker 2's ReplicationPort points at the listener.
        _senderState.ControllerId = 1;
        _senderState.ControllerEpoch = 7;
        _senderState.AddBroker(new BrokerNode { BrokerId = 1, Host = "localhost", Port = 9093 });
        _senderState.AddBroker(new BrokerNode
        {
            BrokerId = 2,
            Host = "127.0.0.1",
            Port = 9094,
            ReplicationPort = _listener.LocalEndPoint.Port,
        });

        var senderConfig = new ClusteringConfig { BrokerId = 1, Host = "localhost", Port = 9093, RebalanceCheckIntervalSeconds = 5 };
        _pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, transport);
        _client = new NativeControllerClient(_pool, _senderState, senderConfig, NullLogger<NativeControllerClient>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _pool.Dispose();
        try { await (_serverTask ?? Task.CompletedTask); }
        catch (OperationCanceledException) { /* server loop ended by test teardown */ }
        await _listener.DisposeAsync();
        await _receiverReplicas.DisposeAsync();
        _cts.Dispose();
    }

    [Fact]
    public async Task SendUpdateMetadata_OverTcp_AppliesStateAndControllerEpochOnReceiver()
    {
        var tp = new TopicPartition { Topic = "orders", Partition = 3 };
        var state = new PartitionState { TopicPartition = tp, LeaderBrokerId = 1, LeaderEpoch = 4, Replicas = [1, 2], Isr = [1] };

        await _client.SendUpdateMetadataAsync([(tp, state)], _cts.Token);

        var applied = _receiverState.GetPartitionState(tp);
        Assert.NotNull(applied);
        Assert.Equal(1, applied!.LeaderBrokerId);
        Assert.Equal(4, applied.LeaderEpoch);
        Assert.Equal(new[] { 1, 2 }, applied.Replicas);

        // The payload carried the sender's controller identity and the receiver stored it.
        Assert.Equal(1, _receiverState.ControllerId);
        Assert.Equal(7, _receiverState.ControllerEpoch);

        // LiveBrokers propagation (#69/Inc5): the push taught the receiver the sender's endpoint.
        var learnedSender = _receiverState.GetBroker(1);
        Assert.NotNull(learnedSender);
        Assert.Equal("localhost", learnedSender!.Host);
    }

    [Fact]
    public async Task SendLeaderAndIsr_OverTcp_ReceiverBecomesLeader()
    {
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        var state = new PartitionState { TopicPartition = tp, LeaderBrokerId = 2, LeaderEpoch = 5, Replicas = [2], Isr = [2] };

        await _client.SendLeaderAndIsrAsync([(tp, state)], _cts.Token);

        Assert.True(_receiverReplicas.IsLeader(tp));
        Assert.Equal(2, _receiverState.GetPartitionState(tp)!.LeaderBrokerId);
    }

    [Fact]
    public async Task NotifyIsrChanged_RemoteController_SendsNativeAlterPartition()
    {
        // Broker 2 (the receiver) is the controller from the sender's point of view.
        _senderState.ControllerId = 2;
        _receiverIsrApplier.IsController = true;
        var tp = new TopicPartition { Topic = "orders", Partition = 1 };
        _receiverIsrApplier.Result = new PartitionState { TopicPartition = tp, LeaderBrokerId = 1, LeaderEpoch = 3, Isr = [1, 2] };

        await _client.NotifyIsrChangedAsync(tp, leaderId: 1, leaderEpoch: 3, isr: [1, 2], _cts.Token);

        Assert.NotNull(_receiverIsrApplier.LastApply);
        var (rTp, leaderId, leaderEpoch) = (_receiverIsrApplier.LastApply!.Value.Tp, _receiverIsrApplier.LastApply.Value.LeaderId, _receiverIsrApplier.LastApply.Value.LeaderEpoch);
        Assert.Equal(tp, rTp);
        Assert.Equal(1, leaderId);
        Assert.Equal(3, leaderEpoch);
        Assert.Equal([1, 2], _receiverIsrApplier.LastApply.Value.NewIsr);
    }

    [Fact]
    public async Task SendStopReplica_UnknownBroker_IsSwallowedBestEffort()
    {
        // Best-effort contract: an unknown target logs and returns, never throws.
        await _client.SendStopReplicaAsync(99, [(new TopicPartition { Topic = "t", Partition = 0 }, 1, false)], _cts.Token);
    }
}

using System.Text.Json;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using HeartbeatRequest = Kuestenlogik.Surgewave.Clustering.Cluster.HeartbeatRequest;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A partition whose last in-sync replica fails has no leader until that replica is back, unless its topic allows an
/// unclean election (#253).
/// </summary>
/// <remarks>
/// The controller took the failed broker out of the ISR, which left none, and elected any live replica: one that was
/// not in sync, so what the failed leader had acknowledged and the new one had not fetched was gone, while the broker
/// told clients <c>unclean.leader.election.enable</c> was <c>false</c>. Kafka keeps the last member in the ISR and
/// leaves the partition without a leader, and so does this. A real single-node Raft: the controller is broker 0, the
/// other replica is broker 7, whose failure and return the test raises itself. Every wait is for the partitions'
/// signal.
/// </remarks>
[Trait("Category", TestCategories.Integration)]
public sealed class LastInSyncReplicaTests : IAsyncLifetime
{
    private const int Self = 0;
    private const int Other = 7;
    private const string Topic = "orders";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);
    private static readonly TopicPartition Tp = new() { Topic = Topic, Partition = 0 };

    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-last-isr-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-last-isr-logs-" + Guid.NewGuid().ToString("N"));

    private readonly ClusteringConfig _config;
    private readonly ClusterState _state = new();
    private readonly LogManager _logs;
    private readonly ClusterController _controller;
    private readonly HeartbeatManager _heartbeats;
    private readonly RaftNode _raftNode;

    public LastInSyncReplicaTests()
    {
        _config = new ClusteringConfig
        {
            BrokerId = Self,
            Host = "localhost",
            Port = 9092,
            ReplicationPort = 10092,
            RaftElectionTimeoutMinMs = 50,
            RaftElectionTimeoutMaxMs = 100,
            RaftHeartbeatIntervalMs = 25,
            RaftPeerDiscoveryTimeoutSeconds = 0,
            RaftDataDirectory = _raftDir,
        };

        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, _state, _logs, _config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        _controller = new ClusterController(NullLogger<ClusterController>.Instance, _state, replicaManager, _config);
        _raftNode = new RaftNode(
            NullLogger<RaftNode>.Instance, _config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, _config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(
                NullLogger<MetadataStateMachine>.Instance, _state, TestRaftNode.NewMembership(_config, _state)));

        // Never started: the test runs the health check itself, so the failure is raised when the test says.
        _heartbeats = new HeartbeatManager(NullLogger<HeartbeatManager>.Instance, _state, _config);
        _controller.SetHeartbeatManager(_heartbeats);
        _controller.SetRaftNode(_raftNode);
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        await _controller.DisposeAsync();
        await _heartbeats.DisposeAsync();
        await _raftNode.DisposeAsync();
        _logs.Dispose();
        try { Directory.Delete(_raftDir, recursive: true); } catch { /* best-effort cleanup */ }
        try { Directory.Delete(_logDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task WhenTheOnlyInSyncReplicaFails_ThePartitionHasNoLeaderAndKeepsItsIsr()
    {
        var ct = TestContext.Current.CancellationToken;
        GivenThePartitionLedByTheOtherBrokerAlone();
        await TakeOfficeAsync(ct);
        var epoch = _state.GetPartitionState(Tp)!.LeaderEpoch;

        await FailTheOtherBrokerAsync(ct);
        await PartitionAsync(state => state.LeaderBrokerId != Other, ct);

        var partition = _state.GetPartitionState(Tp)!;
        Assert.Equal(-1, partition.LeaderBrokerId);
        Assert.Equal(epoch + 1, partition.LeaderEpoch);
        Assert.Equal([Other], _state.GetIsrSnapshot(Tp));
    }

    [Fact]
    public async Task WhenTheLastInSyncReplicaIsBack_ItLeadsAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        GivenThePartitionLedByTheOtherBrokerAlone();
        await TakeOfficeAsync(ct);
        await FailTheOtherBrokerAsync(ct);
        await PartitionAsync(state => state.LeaderBrokerId == -1, ct);
        var epoch = _state.GetPartitionState(Tp)!.LeaderEpoch;

        // Its heartbeat arrives again.
        _ = _heartbeats.ProcessHeartbeat(new HeartbeatRequest(Other, 0, 0, Self, 0));
        await PartitionAsync(state => state.LeaderBrokerId == Other, ct);

        Assert.Equal(epoch + 1, _state.GetPartitionState(Tp)!.LeaderEpoch);
        Assert.Equal([Other], _state.GetIsrSnapshot(Tp));
    }

    [Fact]
    public async Task WhenTheTopicAllowsAnUncleanElection_ALiveReplicaLeadsAndIsTheIsr()
    {
        var ct = TestContext.Current.CancellationToken;
        GivenThePartitionLedByTheOtherBrokerAlone(uncleanLeaderElection: "true");
        await TakeOfficeAsync(ct);

        await FailTheOtherBrokerAsync(ct);
        await PartitionAsync(state => state.LeaderBrokerId == Self, ct);

        // The broker that was in sync is not any more: what it has and the new leader has not is no longer the log.
        await PartitionAsync(_ => _state.GetIsrSnapshot(Tp) is [Self], ct);
    }

    [Fact]
    public async Task AControllerThatTakesOfficeElectsTheLiveInSyncReplicaOfAPartitionWithoutALeader()
    {
        var ct = TestContext.Current.CancellationToken;
        GivenThePartitionLedByTheOtherBrokerAlone();
        // As the controller before left it, and the broker came back while there was none.
        _state.UpdatePartitionState(Tp, state => state.LeaderBrokerId = -1);
        _ = _heartbeats.ProcessHeartbeat(new HeartbeatRequest(Other, 0, 0, Other, 0));

        await _controller.StartAsync(ct);
        await PartitionAsync(state => state.LeaderBrokerId == Other, ct);

        Assert.Equal([Other], _state.GetIsrSnapshot(Tp));
    }

    [Fact]
    public async Task AnElectionElectsNobodyWhereNoInSyncReplicaCanLead_UnlessItIsAnUncleanOne()
    {
        var ct = TestContext.Current.CancellationToken;
        GivenThePartitionLedByTheOtherBrokerAlone();
        await TakeOfficeAsync(ct);
        await FailTheOtherBrokerAsync(ct);
        await PartitionAsync(state => state.LeaderBrokerId == -1, ct);

        // What a preferred election, a rebalance or a reassignment asks for.
        Assert.False(await _controller.ElectLeaderAsync(Tp, preferredLeader: Self, ct));
        Assert.Equal(-1, _state.GetPartitionState(Tp)!.LeaderBrokerId);
        Assert.Equal([Other], _state.GetIsrSnapshot(Tp));

        // What ElectLeaders asks for with the unclean election type.
        Assert.True(await _controller.ElectLeaderAsync(Tp, preferredLeader: null, unclean: true, ct));
        Assert.Equal(Self, _state.GetPartitionState(Tp)!.LeaderBrokerId);
        Assert.Equal([Self], _state.GetIsrSnapshot(Tp));
    }

    [Fact]
    public async Task MetadataSaysThePartitionHasNoLeader()
    {
        var ct = TestContext.Current.CancellationToken;
        GivenThePartitionLedByTheOtherBrokerAlone();
        await _logs.CreateTopicAsync(Topic, 1, cancellationToken: ct);
        await TakeOfficeAsync(ct);
        await FailTheOtherBrokerAsync(ct);
        await PartitionAsync(state => state.LeaderBrokerId == -1, ct);

        var handler = new MetadataApiHandler(new BrokerConfig { BrokerId = Self }, _logs, NullLogger<MetadataApiHandler>.Instance);
        handler.SetClusterState(_state);
        var response = Assert.IsType<MetadataResponse>(await handler.HandleAsync(
            new MetadataRequest
            {
                ApiKey = ApiKey.Metadata,
                ApiVersion = 9,
                CorrelationId = 1,
                ClientId = "client",
                Topics = [new MetadataRequest.MetadataRequestTopic { Name = Topic }],
            },
            new RequestContext { ConnectionState = new ConnectionState("test-host"), ClientId = "client" },
            ct));

        var partition = Assert.Single(Assert.Single(response.Topics).Partitions);
        Assert.Equal(ErrorCode.LeaderNotAvailable, partition.ErrorCode);
        Assert.Equal(-1, partition.LeaderId);
        Assert.Equal([Other], partition.IsrNodes);
    }

    [Fact]
    public async Task ALeaderChangeToNoLeaderInTheLogLeavesTheIsrAsItIs()
    {
        var ct = TestContext.Current.CancellationToken;
        GivenThePartitionLedByTheOtherBrokerAlone();
        await TakeOfficeAsync(ct);
        var epoch = _state.GetPartitionState(Tp)!.LeaderEpoch;

        var data = JsonSerializer.SerializeToUtf8Bytes(
            new LeaderChangedCommand(Topic, 0, -1, epoch + 1), ClusteringJsonContext.Default.LeaderChangedCommand);
        var index = await _raftNode.ProposeAsync(MetadataCommandType.LeaderChanged, data, ct);
        Assert.True(await _raftNode.WaitForCommitOfAsync(index, data, Bound, ct));

        var partition = _state.GetPartitionState(Tp)!;
        Assert.Equal(-1, partition.LeaderBrokerId);
        Assert.Equal(epoch + 1, partition.LeaderEpoch);
        Assert.Equal([Other], _state.GetIsrSnapshot(Tp));
    }

    // Two replicas, of which the other broker leads and is the only one in sync: this broker has fallen behind.
    private void GivenThePartitionLedByTheOtherBrokerAlone(string? uncleanLeaderElection = null)
    {
        _state.AddTopic(new TopicMetadata
        {
            Name = Topic,
            TopicId = Guid.NewGuid(),
            PartitionCount = 1,
            ReplicationFactor = 2,
            Config = uncleanLeaderElection is null ? [] : new() { ["unclean.leader.election.enable"] = uncleanLeaderElection },
            CreatedAt = DateTime.UtcNow,
        });
        _state.AssignReplicas(Tp, [Other, Self], 1);
        _state.ElectLeader(Tp, Other);
        _state.UpdateIsr(Tp, [Other]);

        // Watched and alive, as a broker whose heartbeats arrive.
        _ = _heartbeats.ProcessHeartbeat(new HeartbeatRequest(Other, 0, 0, Other, 0));
    }

    private async Task TakeOfficeAsync(CancellationToken ct)
    {
        await _controller.StartAsync(ct);
        while (true)
        {
            var changed = _state.MembershipChanged;
            if (_controller.IsController)
                return;
            await changed.WaitAsync(Bound, ct);
        }
    }

    // Past the failure count: the health check reports it failed, to the controller.
    private async Task FailTheOtherBrokerAsync(CancellationToken ct)
    {
        for (var i = 0; i < _config.MaxHeartbeatFailures; i++)
            _heartbeats.AllBrokerHealth[Other].RecordFailure();
        await _heartbeats.CheckBrokerHealthAsync(ct);
        Assert.False(_heartbeats.IsBrokerAlive(Other));
    }

    private async Task PartitionAsync(Func<PartitionState, bool> reached, CancellationToken ct)
    {
        while (true)
        {
            var changed = _state.PartitionsChanged;
            if (reached(_state.GetPartitionState(Tp)!))
                return;
            await changed.WaitAsync(Bound, ct);
        }
    }
}

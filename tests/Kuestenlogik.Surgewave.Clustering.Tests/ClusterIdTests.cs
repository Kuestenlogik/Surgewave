using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// One cluster id per cluster (#203): the first controller writes it to the metadata log, every broker adopts and
/// persists it, and a broker carrying another cluster's id is refused at registration with INCONSISTENT_CLUSTER_ID.
/// Each broker generated and persisted its own id before, so a cluster had none it could report.
/// </summary>
public sealed class ClusterIdTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-cluster-id-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private string Dir(string name) => Path.Combine(_root, name);

    private static ClusterIdManager Identity(string dataDirectory, string? configured = null)
        => new(configured, dataDirectory, NullLogger<ClusterIdManager>.Instance);

    private static string? Persisted(string dataDirectory)
    {
        var path = Path.Combine(dataDirectory, "cluster.id");
        return File.Exists(path) ? File.ReadAllText(path) : null;
    }

    // ---- ClusterIdManager: the broker's own id ----

    [Fact]
    public void ABrokerThatNeverJoinedHasNoIdAndDoesNotGenerateOne()
    {
        var identity = Identity(Dir("a"));

        Assert.Null(identity.ClusterId);
        Assert.False(identity.Known.IsCompleted);
        Assert.Null(Persisted(Dir("a")));
    }

    [Fact]
    public void TheIdAControllerProposesIsAKafkaClusterIdAndTheSameOnEveryCall()
    {
        var identity = Identity(Dir("a"));

        var proposed = identity.ProposeClusterId();

        // Kafka's format: 16 random bytes, URL-safe Base64 without padding.
        Assert.Equal(22, proposed.Length);
        Assert.Matches("^[A-Za-z0-9_-]{22}$", proposed);
        Assert.Equal(proposed, identity.ProposeClusterId());
        Assert.Null(Persisted(Dir("a"))); // persisted once committed, not when proposed
    }

    [Fact]
    public async Task AnAdoptedIdIsPersistedAndLoadedAfterARestart()
    {
        var identity = Identity(Dir("a"));

        Assert.True(identity.Adopt("committed-id"));

        Assert.Equal("committed-id", await identity.Known.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.Equal("committed-id", Persisted(Dir("a")));
        Assert.Equal("committed-id", Identity(Dir("a")).ClusterId);
    }

    [Fact]
    public void ABrokerWithAnotherIdDoesNotAdoptTheCommittedOne()
    {
        Identity(Dir("a")).Adopt("own-id");
        var identity = Identity(Dir("a"));

        Assert.False(identity.Adopt("other-cluster"));

        Assert.Equal("own-id", identity.ClusterId);
        Assert.Equal("own-id", Persisted(Dir("a")));
    }

    [Fact]
    public void AConfiguredIdIsProposedAndPersistedOnceItIsTheClusters()
    {
        var identity = Identity(Dir("a"), configured: "configured-id");

        Assert.Equal("configured-id", identity.ProposeClusterId());
        Assert.Null(Persisted(Dir("a")));

        Assert.True(identity.Adopt("configured-id"));
        Assert.Equal("configured-id", Persisted(Dir("a")));
    }

    [Fact]
    public void AConfiguredIdThatIsNotThePersistedOneStopsTheBrokerAtStartup()
    {
        Identity(Dir("a")).Adopt("persisted-id");

        var ex = Assert.Throws<InvalidOperationException>(() => Identity(Dir("a"), configured: "configured-id"));

        Assert.Contains("configured-id", ex.Message, StringComparison.Ordinal);
        Assert.Contains("persisted-id", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABrokerWithoutAClusterGeneratesItsIdOnceAndKeepsIt()
    {
        var generated = Identity(Dir("a")).EnsureClusterId();

        Assert.Equal(generated, Persisted(Dir("a")));
        Assert.Equal(generated, Identity(Dir("a")).EnsureClusterId());
    }

    // ---- The metadata log: written once, adopted by every broker ----

    private (MetadataStateMachine Machine, ClusterState State, ClusterIdManager Identity) NewStateMachine(string dataDirectory)
    {
        var state = new ClusterState();
        var identity = Identity(dataDirectory);
        var membership = new ClusterMembershipService(identity, state, NullLogger<ClusterMembershipService>.Instance);
        return (new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, membership), state, identity);
    }

    private static RaftLogEntry ClusterIdEntry(long index, string clusterId) => new()
    {
        Term = 1,
        Index = index,
        CommandType = MetadataCommandType.ClusterIdSet,
        Data = JsonSerializer.SerializeToUtf8Bytes(new ClusterIdSetCommand(clusterId), ClusteringJsonContext.Default.ClusterIdSetCommand),
    };

    [Fact]
    public async Task ABrokerAppliesTheCommittedIdAndAdoptsIt()
    {
        var (machine, state, identity) = NewStateMachine(Dir("broker"));

        machine.Apply(ClusterIdEntry(2, "the-cluster"));

        Assert.Equal("the-cluster", await state.ClusterIdCommitted.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.Equal("the-cluster", identity.ClusterId);
        Assert.Equal("the-cluster", Persisted(Dir("broker")));
    }

    [Fact]
    public void TheFirstIdInTheLogIsTheClustersOnEveryBroker()
    {
        var (machine, state, identity) = NewStateMachine(Dir("broker"));

        machine.Apply(ClusterIdEntry(2, "first"));
        machine.Apply(ClusterIdEntry(7, "second"));

        Assert.Equal("first", state.ClusterId);
        Assert.Equal("first", identity.ClusterId);
        Assert.Equal("first", Persisted(Dir("broker")));
    }

    [Fact]
    public async Task ASnapshotCarriesTheClusterId()
    {
        var ct = TestContext.Current.CancellationToken;
        var (source, _, _) = NewStateMachine(Dir("source"));
        source.Apply(ClusterIdEntry(2, "the-cluster"));
        var snapshot = await source.CreateSnapshotAsync(ct);

        var (restored, state, identity) = NewStateMachine(Dir("restored"));
        await restored.RestoreFromSnapshotAsync(snapshot, ct);

        Assert.Equal("the-cluster", state.ClusterId);
        Assert.Equal("the-cluster", identity.ClusterId);
    }

    // ---- Registration: checked against the id in the log ----

    private static (BrokerRegistrationCoordinator Coordinator, ClusterState State, ClusterMembershipService Membership) NewController(string dataDirectory)
    {
        var state = new ClusterState { ControllerId = 1 };
        var membership = new ClusterMembershipService(Identity(dataDirectory), state, NullLogger<ClusterMembershipService>.Instance);
        var coordinator = new BrokerRegistrationCoordinator(
            membership, new TestBrokerRegistrar(membership), state, localBrokerId: 1,
            NullLogger<BrokerRegistrationCoordinator>.Instance);
        return (coordinator, state, membership);
    }

    private static BrokerRegistrationInput Registration(string clusterId) => new(
        BrokerId: 2, ClusterId: clusterId, IncarnationId: Guid.NewGuid(),
        Listeners: [new ListenerSpec("PLAINTEXT", "h", 9094, 0)],
        Features: [InterBrokerProtocolFeature.LocalFeatureSpec], Rack: null, PreviousBrokerEpoch: -1);

    [Fact]
    public async Task ABrokerWithAnotherClustersIdIsRefusedWithInconsistentClusterIdAndNotRegistered()
    {
        var (coordinator, state, membership) = NewController(Dir("controller"));
        state.TrySetClusterId("the-cluster");

        var outcome = await coordinator.RegisterAsync(Registration("other-cluster"), TestContext.Current.CancellationToken);

        Assert.Equal(ClusterRpcStatus.InconsistentClusterId, outcome.Status);
        Assert.Equal(104, (short)outcome.Status); // Kafka's INCONSISTENT_CLUSTER_ID
        Assert.False(membership.TryGetBrokerEpoch(2, out _));
    }

    [Theory]
    [InlineData("the-cluster")]
    [InlineData("")] // a broker that has never joined a cluster adopts the id from the log
    public async Task ABrokerWithTheClustersIdOrNoneIsRegistered(string announced)
    {
        var (coordinator, state, _) = NewController(Dir("controller"));
        state.TrySetClusterId("the-cluster");

        var outcome = await coordinator.RegisterAsync(Registration(announced), TestContext.Current.CancellationToken);

        Assert.Equal(ClusterRpcStatus.None, outcome.Status);
    }

    [Fact]
    public async Task RegistrationWaitsUntilTheControllerHasCommittedTheId()
    {
        var (coordinator, _, membership) = NewController(Dir("controller"));

        var outcome = await coordinator.RegisterAsync(Registration("the-cluster"), TestContext.Current.CancellationToken);

        Assert.Equal(ClusterRpcStatus.BrokerNotAvailable, outcome.Status); // retried by the joiner
        Assert.False(membership.TryGetBrokerEpoch(2, out _));
    }

    // ---- The joining broker: announces its id, and learns that it was refused ----

    private sealed class RefusingRpc : IBrokerLifecycleRpc
    {
        public readonly TaskCompletionSource<string> Announced = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<BrokerRegistrationOutcome> RegisterAsync(BrokerRegistrationInput input, CancellationToken ct = default)
        {
            Announced.TrySetResult(input.ClusterId);
            return Task.FromResult(new BrokerRegistrationOutcome(ClusterRpcStatus.InconsistentClusterId, -1));
        }

        public Task<BrokerHeartbeatOutcome> HeartbeatAsync(BrokerHeartbeatInput input, CancellationToken ct = default)
            => throw new InvalidOperationException("a refused broker never heartbeats");
    }

    [Fact]
    public async Task AJoiningBrokerAnnouncesItsPersistedIdAndLearnsItWasRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        Identity(Dir("joiner")).Adopt("persisted-id");
        var identity = Identity(Dir("joiner"));
        var rpc = new RefusingRpc();
        var config = new ClusteringConfig
        {
            BrokerId = 2, Host = "localhost", Port = 9094, ReplicationPort = 10094,
            ClusterNodes = "1:localhost:9092", HeartbeatIntervalMs = 30,
        };
        await using var loop = new BrokerLifecycleLoop(
            rpc, config, NullLogger<BrokerLifecycleLoop>.Instance, clusterIdentity: identity);

        await loop.StartAsync(ct);

        Assert.Equal("persisted-id", await rpc.Announced.Task.WaitAsync(Timeout, ct));
        Assert.Equal("persisted-id", await identity.Refused.WaitAsync(Timeout, ct));
        Assert.Equal(-1, loop.BrokerEpoch);
    }

    // ---- The controller: writes the id once, a later controller finds it ----

    private sealed class SingleNodeController : IAsyncDisposable
    {
        private readonly LogManager _logs;
        private readonly ReplicaManager _replicas;

        public SingleNodeController(string raftDirectory, string dataDirectory, string? configuredClusterId = null)
        {
            var config = new ClusteringConfig
            {
                BrokerId = 1, Host = "localhost", Port = 9092, ReplicationPort = 10092,
                RaftElectionTimeoutMinMs = 50, RaftElectionTimeoutMaxMs = 100, RaftHeartbeatIntervalMs = 25,
                RaftDataDirectory = raftDirectory, DataDirectory = dataDirectory, ClusterId = configuredClusterId,
            };
            State = new ClusterState();
            _logs = new LogManager(dataDirectory + "-logs", new MemoryLogSegmentFactory());
            _replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, State, _logs, config,
                new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
            Identity = new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance);
            var membership = new ClusterMembershipService(Identity, State, NullLogger<ClusterMembershipService>.Instance);
            Controller = new ClusterController(NullLogger<ClusterController>.Instance, State, _replicas, config);
            Raft = new RaftNode(
                NullLogger<RaftNode>.Instance, config,
                new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
                new SingleNodeRaftTransport(),
                new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, State, membership));
            Controller.SetRaftNode(Raft);
            Controller.SetClusterIdentity(Identity);
        }

        public ClusterState State { get; }
        public ClusterIdManager Identity { get; }
        public ClusterController Controller { get; }
        public RaftNode Raft { get; }

        public async ValueTask DisposeAsync()
        {
            await Raft.DisposeAsync();
            await Controller.DisposeAsync();
            await _replicas.DisposeAsync();
            _logs.Dispose();
        }
    }

    [Fact]
    public async Task TheFirstControllerWritesAndPersistsTheIdAndALaterOneFindsItInTheLog()
    {
        var ct = TestContext.Current.CancellationToken;
        string written;
        await using (var first = new SingleNodeController(Dir("raft"), Dir("data-1")))
        {
            await first.Controller.StartAsync(ct);
            written = await first.State.ClusterIdCommitted.WaitAsync(Timeout, ct);

            Assert.Equal(written, first.Identity.ClusterId);
            Assert.Equal(written, Persisted(Dir("data-1")));
        }

        // The same metadata log, a data directory without a cluster.id: the id comes from the log, not a new one.
        await using var later = new SingleNodeController(Dir("raft"), Dir("data-2"));
        await later.Controller.StartAsync(ct);

        Assert.Equal(written, await later.State.ClusterIdCommitted.WaitAsync(Timeout, ct));
        Assert.Equal(written, await later.Identity.Known.WaitAsync(Timeout, ct));
        Assert.Equal(written, Persisted(Dir("data-2")));
    }

    [Fact]
    public async Task AConfiguredIdIsTheOneTheControllerWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var controller = new SingleNodeController(Dir("raft"), Dir("data"), configuredClusterId: "configured-id");

        await controller.Controller.StartAsync(ct);

        Assert.Equal("configured-id", await controller.State.ClusterIdCommitted.WaitAsync(Timeout, ct));
        Assert.Equal("configured-id", Persisted(Dir("data")));
    }
}

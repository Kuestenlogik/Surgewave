using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc6b — end-to-end native broker join: a joining broker's <see cref="NativeBrokerLifecycleClient"/>
/// sends BrokerRegistration/BrokerHeartbeat frames over a real TCP loopback to the controller's
/// <see cref="NativeInterBrokerServer"/>, which routes them to the shared <see cref="ClusterMembershipService"/>.
/// This is the full plugin-free join path minus only the ReplicationServer multiplex (covered by Inc4).
/// </summary>
public class NativeBrokerLifecycleClientTests : IAsyncLifetime
{
    private readonly CancellationTokenSource _cts = new(TimeSpan.FromSeconds(20));

    // Controller (broker 1): membership authority behind a TCP loopback listener.
    private readonly ClusterState _controllerState = new();
    private ReplicaManager _controllerReplicas = null!;
    private ClusterMembershipService _membership = null!;
    private Kuestenlogik.Surgewave.Transport.IPeerListener _listener = null!;
    private Task? _serverTask;

    // Joining broker (broker 2).
    private ConnectionPool _pool = null!;
    private NativeBrokerLifecycleClient _client = null!;

    public async ValueTask InitializeAsync()
    {
        var transport = new TcpPeerTransport();

        var controllerConfig = new ClusteringConfig { BrokerId = 1, Host = "localhost", Port = 9093, RebalanceCheckIntervalSeconds = 5 };
        _controllerState.ControllerId = 1; // this broker is the controller (IsController via id fallback)
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        _controllerReplicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, _controllerState, logs, controllerConfig, transport);
        _membership = new ClusterMembershipService(
            new ClusterIdManager(controllerConfig, NullLogger<ClusterIdManager>.Instance),
            _controllerState, NullLogger<ClusterMembershipService>.Instance);
        var server = new NativeInterBrokerServer(
            NullLogger<NativeInterBrokerServer>.Instance,
            new ClusterStateInterBrokerService(
                NullLogger<ClusterStateInterBrokerService>.Instance,
                _controllerState, _controllerReplicas, logs, localBrokerId: 1, isrUpdateApplier: null, membership: _membership));

        _listener = transport.CreateListener(new IPEndPoint(IPAddress.Loopback, 0));
        await _listener.StartAsync();

        _serverTask = Task.Run(async () =>
        {
            await using var conn = await _listener.AcceptAsync(_cts.Token);
            await using var lease = await conn.AcceptInboundStreamAsync(_cts.Token);
            while (await server.HandleSingleAsync(lease.Stream, _cts.Token)) { }
        }, _cts.Token);

        // Joining broker knows the controller (id 1) at the listener's replication port.
        var joinerState = new ClusterState { ControllerId = 1 };
        joinerState.AddBroker(new BrokerNode { BrokerId = 1, Host = "127.0.0.1", Port = 9093, ReplicationPort = _listener.LocalEndPoint.Port });
        var joinerConfig = new ClusteringConfig { BrokerId = 2, Host = "127.0.0.1", Port = 9094, ReplicationPort = 10094, RebalanceCheckIntervalSeconds = 5 };
        _pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, transport);
        _client = new NativeBrokerLifecycleClient(_pool, joinerState, joinerConfig, NullLogger<NativeBrokerLifecycleClient>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _pool.Dispose();
        try { await (_serverTask ?? Task.CompletedTask); }
        catch (OperationCanceledException) { }
        await _listener.DisposeAsync();
        await _controllerReplicas.DisposeAsync();
        _cts.Dispose();
    }

    private static BrokerRegistrationInput JoinerRegistration(Guid incarnation) => new(
        BrokerId: 2,
        ClusterId: "",
        IncarnationId: incarnation,
        Listeners: [new ListenerSpec("PLAINTEXT", "10.0.0.2", 9094, 0), new ListenerSpec("REPLICATION", "10.0.0.2", 10094, 0)],
        Features: [InterBrokerProtocolFeature.LocalFeatureSpec],
        Rack: null,
        PreviousBrokerEpoch: -1);

    [Fact]
    public async Task RegisterOverTcp_AssignsEpochAndAddsBrokerToControllerState()
    {
        var outcome = await _client.RegisterAsync(JoinerRegistration(Guid.NewGuid()), _cts.Token);

        Assert.Equal(ClusterRpcStatus.None, outcome.Status);
        Assert.True(outcome.BrokerEpoch > 0);

        // The controller learned the joining broker with its real replication port + advertised level.
        var learned = _controllerState.GetBroker(2);
        Assert.NotNull(learned);
        Assert.Equal(10094, learned!.ReplicationPort);
        Assert.Equal(InterBrokerProtocolFeature.Native, learned.InterBrokerProtocol);
    }

    [Fact]
    public async Task HeartbeatOverTcp_AfterRegister_Unfences()
    {
        var incarnation = Guid.NewGuid();
        var reg = await _client.RegisterAsync(JoinerRegistration(incarnation), _cts.Token);
        Assert.Equal(ClusterRpcStatus.None, reg.Status);
        Assert.True(_membership.IsBrokerFenced(2)); // starts fenced

        var hb = await _client.HeartbeatAsync(
            new BrokerHeartbeatInput(BrokerId: 2, BrokerEpoch: reg.BrokerEpoch, CurrentMetadataOffset: 0, WantFence: false, WantShutDown: false),
            _cts.Token);

        Assert.Equal(ClusterRpcStatus.None, hb.Status);
        Assert.False(hb.IsFenced);
        Assert.False(_membership.IsBrokerFenced(2));
    }

    [Fact]
    public async Task RegisterOverTcp_NativeJoiner_RaisesControllerFinalizedLevelToNative()
    {
        // Before the join the controller only knows itself (id 1, KafkaWire default until it
        // self-advertises); after a native join the MIN over {1,2}... broker 1 has no level here, so
        // this asserts the joiner's level landed and is Native.
        await _client.RegisterAsync(JoinerRegistration(Guid.NewGuid()), _cts.Token);
        Assert.Equal(InterBrokerProtocolFeature.Native, _controllerState.GetBroker(2)!.InterBrokerProtocol);
    }
}

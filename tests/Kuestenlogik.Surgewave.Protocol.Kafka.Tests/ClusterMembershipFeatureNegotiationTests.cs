using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// #60 Inc3 — controller-side coverage: a broker's advertised inter.broker.protocol feature is stored on
/// <see cref="BrokerNode.InterBrokerProtocol"/> and the cluster-wide finalized level (the MIN across brokers)
/// reflects it. An older broker that omits the feature keeps the whole cluster pinned to the Kafka wire.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ClusterMembershipFeatureNegotiationTests
{
    private static readonly RequestContext Ctx =
        new() { ConnectionState = new ConnectionState("ibp-test"), ClientId = "broker" };

    private static (ClusterMembershipHandler handler, ClusterState state) NewController()
    {
        var config = new ClusteringConfig();
        var clusterIdManager = new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance);
        var state = new ClusterState();
        var handler = new ClusterMembershipHandler(clusterIdManager, state, NullLogger<ClusterMembershipHandler>.Instance);
        return (handler, state);
    }

    private static BrokerRegistrationRequest.Feature InterBrokerProtocol(short max) => new()
    {
        Name = InterBrokerProtocolFeature.FeatureName,
        MinSupportedVersion = InterBrokerProtocolFeature.KafkaWire,
        MaxSupportedVersion = max
    };

    private static BrokerRegistrationRequest Registration(int brokerId, params BrokerRegistrationRequest.Feature[] features)
        => Registration(brokerId, replicationPort: null, features);

    private static BrokerRegistrationRequest Registration(int brokerId, ushort? replicationPort, params BrokerRegistrationRequest.Feature[] features) => new()
    {
        ApiKey = ApiKey.BrokerRegistration,
        ApiVersion = 3,
        CorrelationId = 1,
        ClientId = $"broker-{brokerId}",
        BrokerId = brokerId,
        ClusterId = "", // empty → ValidateClusterId short-circuits to true, keeps the test hermetic (no disk I/O)
        IncarnationId = Guid.NewGuid(),
        Listeners =
        [
            new BrokerRegistrationRequest.Listener { Name = "PLAINTEXT", Host = "h", Port = (ushort)(9092 + brokerId), SecurityProtocol = 0 },
            .. replicationPort is { } rp
                ? new[] { new BrokerRegistrationRequest.Listener { Name = "REPLICATION", Host = "h", Port = rp, SecurityProtocol = 0 } }
                : Array.Empty<BrokerRegistrationRequest.Listener>(),
        ],
        Features = [.. features],
        Rack = null,
    };

    private static async Task<BrokerRegistrationResponse> Register(ClusterMembershipHandler handler, BrokerRegistrationRequest req)
        => (BrokerRegistrationResponse)await handler.HandleAsync(req, Ctx, CancellationToken.None);

    [Fact]
    public async Task Register_NativeBroker_StoresNativeLevelAndFinalizesNative()
    {
        var (handler, state) = NewController();

        var resp = await Register(handler, Registration(1, InterBrokerProtocol(InterBrokerProtocolFeature.Native)));

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.GetBroker(1)!.InterBrokerProtocol);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task Register_OlderBrokerWithoutFeature_StoresKafkaWire()
    {
        var (handler, state) = NewController();

        await Register(handler, Registration(1)); // no inter.broker.protocol feature advertised

        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.GetBroker(1)!.InterBrokerProtocol);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task Register_MixedNativeAndOlder_FinalizesKafkaWire()
    {
        var (handler, state) = NewController();

        await Register(handler, Registration(1, InterBrokerProtocol(InterBrokerProtocolFeature.Native)));
        await Register(handler, Registration(2)); // older broker, no feature

        Assert.Equal(InterBrokerProtocolFeature.Native, state.GetBroker(1)!.InterBrokerProtocol);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.GetBroker(2)!.InterBrokerProtocol);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task Reregistration_WithUpgradedFeature_UpdatesStoredLevel()
    {
        var (handler, state) = NewController();

        // First join as an older broker (no feature), then re-register advertising native (a rolling upgrade).
        await Register(handler, Registration(1));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.GetBroker(1)!.InterBrokerProtocol);

        await Register(handler, Registration(1, InterBrokerProtocol(InterBrokerProtocolFeature.Native)));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.GetBroker(1)!.InterBrokerProtocol);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task Reregistration_WithDowngradedFeature_UpdatesStoredLevel()
    {
        var (handler, state) = NewController();

        // #72 Inc1 — down twin of the upgrade test: a broker re-registers WITHOUT the feature (a
        // rolling downgrade to an older build). The registration authority must converge the stored
        // level DOWN, or the finalized MIN would keep selecting the native wire against a peer that
        // no longer speaks it.
        await Register(handler, Registration(1, InterBrokerProtocol(InterBrokerProtocolFeature.Native)));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);

        await Register(handler, Registration(1));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.GetBroker(1)!.InterBrokerProtocol);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }

    // ── #60 Inc5: the ReplicationPort resolution the native controller client depends on ────────

    [Fact]
    public async Task Register_WithReplicationListener_StoresRealReplicationPortAndClientPort()
    {
        var (handler, state) = NewController();

        await Register(handler, Registration(1, replicationPort: 10999, InterBrokerProtocol(InterBrokerProtocolFeature.Native)));

        var node = state.GetBroker(1)!;
        // Host/Port must come from the CLIENT listener, the replication port from the REPLICATION
        // listener — FirstOrDefault would conflate the two depending on listener order.
        Assert.Equal(9093, node.Port);
        Assert.Equal(10999, node.ReplicationPort);
    }

    [Fact]
    public async Task Register_WithoutReplicationListener_KeepsPreviouslyDiscoveredReplicationPort()
    {
        var (handler, state) = NewController();

        // The node was discovered from cluster-node config with its real replication port (#69);
        // a registration without a REPLICATION listener must not clobber it back to port + 1000.
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "h", Port = 9093, ReplicationPort = 12345 });

        await Register(handler, Registration(1));

        Assert.Equal(12345, state.GetBroker(1)!.ReplicationPort);
    }
}

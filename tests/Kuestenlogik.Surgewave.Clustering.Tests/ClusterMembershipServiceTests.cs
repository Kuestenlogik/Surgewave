using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc6b — the protocol-neutral membership authority: incarnation-keyed epoch assignment,
/// fence-until-caught-up heartbeats, and the REPLICATION-listener → BrokerNode.ReplicationPort
/// resolution the native controller client depends on.
/// </summary>
public class ClusterMembershipServiceTests
{
    private static (ClusterMembershipService Service, ClusterState State) NewService()
    {
        var config = new ClusteringConfig { BrokerId = 0 };
        var state = new ClusterState();
        var idManager = new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance);
        var service = new ClusterMembershipService(idManager, state, NullLogger<ClusterMembershipService>.Instance);
        return (service, state);
    }

    // ClusterId "" short-circuits ValidateClusterId to true, keeping tests hermetic (no cluster.id file).
    private static BrokerRegistrationInput Registration(int brokerId, Guid incarnation, short level = InterBrokerProtocolFeature.Native, int? replicationPort = null)
    {
        var listeners = new List<ListenerSpec> { new("PLAINTEXT", "h", 9092 + brokerId, 0) };
        if (replicationPort is { } rp)
            listeners.Add(new ListenerSpec("REPLICATION", "h", rp, 0));

        return new BrokerRegistrationInput(
            BrokerId: brokerId,
            ClusterId: "",
            IncarnationId: incarnation,
            Listeners: listeners,
            Features: [new FeatureSpec(InterBrokerProtocolFeature.FeatureName, InterBrokerProtocolFeature.KafkaWire, level)],
            Rack: null,
            PreviousBrokerEpoch: -1);
    }

    [Fact]
    public void Register_AssignsMonotonicEpochsAndStoresBroker()
    {
        var (service, state) = NewService();

        var r1 = service.Register(Registration(1, Guid.NewGuid(), replicationPort: 10999));
        var r2 = service.Register(Registration(2, Guid.NewGuid()));

        Assert.Equal(ClusterRpcStatus.None, r1.Status);
        Assert.Equal(ClusterRpcStatus.None, r2.Status);
        Assert.True(r2.BrokerEpoch > r1.BrokerEpoch);

        var node1 = state.GetBroker(1)!;
        Assert.Equal(10999, node1.ReplicationPort);                     // from the REPLICATION listener
        Assert.Equal(InterBrokerProtocolFeature.Native, node1.InterBrokerProtocol);
        Assert.Equal(9093, node1.Port);                                 // client listener 9092+1
    }

    [Fact]
    public void Reregister_SameIncarnation_KeepsEpoch()
    {
        var (service, _) = NewService();
        var incarnation = Guid.NewGuid();

        var first = service.Register(Registration(1, incarnation));
        var again = service.Register(Registration(1, incarnation));

        Assert.Equal(first.BrokerEpoch, again.BrokerEpoch);
    }

    [Fact]
    public void Reregister_NewIncarnation_AssignsFreshEpoch()
    {
        var (service, _) = NewService();

        var first = service.Register(Registration(1, Guid.NewGuid()));
        var restarted = service.Register(Registration(1, Guid.NewGuid())); // restart → new incarnation

        Assert.True(restarted.BrokerEpoch > first.BrokerEpoch);
    }

    [Fact]
    public void Register_WithoutReplicationListener_DerivesReplicationPortFromClientPort()
    {
        var (service, state) = NewService();

        service.Register(Registration(3, Guid.NewGuid())); // no REPLICATION listener

        // Client port 9092+3 = 9095, replication defaults to +1000.
        Assert.Equal(9095 + 1000, state.GetBroker(3)!.ReplicationPort);
    }

    [Fact]
    public void Heartbeat_UnknownBroker_IsBrokerNotAvailable()
    {
        var (service, _) = NewService();

        var outcome = service.Heartbeat(new BrokerHeartbeatInput(99, 1, 0, false, false));

        Assert.Equal(ClusterRpcStatus.BrokerNotAvailable, outcome.Status);
        Assert.True(outcome.IsFenced);
    }

    [Fact]
    public void Heartbeat_StaleEpoch_IsStaleBrokerEpoch()
    {
        var (service, _) = NewService();
        var reg = service.Register(Registration(1, Guid.NewGuid()));

        var outcome = service.Heartbeat(new BrokerHeartbeatInput(1, reg.BrokerEpoch + 99, 0, false, false));

        Assert.Equal(ClusterRpcStatus.StaleBrokerEpoch, outcome.Status);
    }

    [Fact]
    public void Heartbeat_CaughtUp_Unfences()
    {
        var (service, _) = NewService();
        var reg = service.Register(Registration(1, Guid.NewGuid()));
        Assert.True(service.IsBrokerFenced(1)); // starts fenced

        // A heartbeat at a non-negative metadata offset with WantFence=false unfences.
        var outcome = service.Heartbeat(new BrokerHeartbeatInput(1, reg.BrokerEpoch, CurrentMetadataOffset: 0, WantFence: false, WantShutDown: false));

        Assert.Equal(ClusterRpcStatus.None, outcome.Status);
        Assert.True(outcome.IsCaughtUp);
        Assert.False(outcome.IsFenced);
        Assert.False(service.IsBrokerFenced(1));
    }

    [Fact]
    public void Register_NativePeer_RaisesFinalizedLevel()
    {
        var (service, state) = NewService();

        service.Register(Registration(1, Guid.NewGuid(), level: InterBrokerProtocolFeature.Native));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);

        // A KafkaWire peer joining drops the finalized MIN back to KafkaWire.
        service.Register(Registration(2, Guid.NewGuid(), level: InterBrokerProtocolFeature.KafkaWire));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }
}

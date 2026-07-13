using Kuestenlogik.Surgewave.Clustering.Cluster;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc3 — coverage for the IBP-style inter-broker-protocol feature: the neutral
/// <see cref="InterBrokerProtocolFeature"/> helpers, the per-broker <see cref="BrokerNode.InterBrokerProtocol"/>
/// field, and the cluster-wide MIN finalization on <see cref="ClusterState.FinalizedInterBrokerProtocol"/>.
/// The finalized level is the safety anchor — a single broker that cannot speak native pins the whole
/// cluster to the Kafka wire.
/// </summary>
public class InterBrokerProtocolNegotiationTests
{
    private static BrokerNode Broker(int id, short level) => new()
    {
        BrokerId = id,
        Host = "localhost",
        Port = 9092 + id,
        InterBrokerProtocol = level
    };

    [Fact]
    public void BrokerNode_DefaultInterBrokerProtocol_IsKafkaWire()
    {
        var node = new BrokerNode { BrokerId = 1, Host = "h", Port = 9092 };
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, node.InterBrokerProtocol);
    }

    [Fact]
    public void LocalFeatureSpec_AdvertisesKafkaWireToLocalMax()
    {
        var spec = InterBrokerProtocolFeature.LocalFeatureSpec;
        Assert.Equal(InterBrokerProtocolFeature.FeatureName, spec.Name);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, spec.MinSupportedVersion);
        Assert.Equal(InterBrokerProtocolFeature.LocalMaxSupported, spec.MaxSupportedVersion);
    }

    [Fact]
    public void LevelFrom_FeaturePresent_ReturnsMaxSupportedVersion()
    {
        IReadOnlyList<FeatureSpec> features =
        [
            new("metadata.version", 1, 20),
            new(InterBrokerProtocolFeature.FeatureName, InterBrokerProtocolFeature.KafkaWire, InterBrokerProtocolFeature.Native),
        ];
        Assert.Equal(InterBrokerProtocolFeature.Native, InterBrokerProtocolFeature.LevelFrom(features));
    }

    [Fact]
    public void LevelFrom_FeatureAbsent_ReturnsKafkaWire()
    {
        IReadOnlyList<FeatureSpec> features = [new("metadata.version", 1, 20)];
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, InterBrokerProtocolFeature.LevelFrom(features));
    }

    [Fact]
    public void LevelFrom_EmptyFeatures_ReturnsKafkaWire()
    {
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, InterBrokerProtocolFeature.LevelFrom([]));
    }

    [Fact]
    public void FinalizedInterBrokerProtocol_EmptyCluster_ReturnsKafkaWire()
    {
        var state = new ClusterState();
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public void FinalizedInterBrokerProtocol_SingleNativeBroker_ReturnsNative()
    {
        var state = new ClusterState();
        state.AddBroker(Broker(1, InterBrokerProtocolFeature.Native));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public void FinalizedInterBrokerProtocol_AllNative_ReturnsNative()
    {
        var state = new ClusterState();
        state.AddBroker(Broker(1, InterBrokerProtocolFeature.Native));
        state.AddBroker(Broker(2, InterBrokerProtocolFeature.Native));
        state.AddBroker(Broker(3, InterBrokerProtocolFeature.Native));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public void FinalizedInterBrokerProtocol_OneOldPeer_PinsToKafkaWire()
    {
        // Safety anchor: two native brokers + one older broker (absent feature = KafkaWire) pins the cluster.
        var state = new ClusterState();
        state.AddBroker(Broker(1, InterBrokerProtocolFeature.Native));
        state.AddBroker(Broker(2, InterBrokerProtocolFeature.Native));
        state.AddBroker(Broker(3, InterBrokerProtocolFeature.KafkaWire));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public void FinalizedInterBrokerProtocol_RisesWhenOldPeerLeaves()
    {
        var state = new ClusterState();
        state.AddBroker(Broker(1, InterBrokerProtocolFeature.Native));
        state.AddBroker(Broker(2, InterBrokerProtocolFeature.KafkaWire));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);

        // Old peer leaves → the cluster can rise to native.
        state.RemoveBroker(2);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public void RegisterBroker_HelperDefaultsToKafkaWire()
    {
        // The rack-only RegisterBroker overload carries no feature info, so it must default to the anchor.
        var state = new ClusterState();
        state.RegisterBroker(1, "localhost", 9092, "rack-1");
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.GetBroker(1)!.InterBrokerProtocol);
    }
}

using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc5 — the local broker node's self-advertisement. The finalized inter-broker protocol level
/// is the MIN over ALL nodes including the local one, so the local node must carry
/// <see cref="InterBrokerProtocolFeature.LocalMaxSupported"/> and its real replication port — and
/// keep them when the cluster-nodes config (which routinely lists the local broker too) is parsed
/// right after registration.
/// </summary>
public class ClusterControllerLocalNodeTests
{
    private static (ClusterController Controller, ClusterState State) NewController(ClusteringConfig config)
    {
        var state = new ClusterState();
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var controller = new ClusterController(
            NullLogger<ClusterController>.Instance, state, replicaManager, config);
        return (controller, state);
    }

    [Fact]
    public async Task StartAsync_ClusterNodesListingSelf_LocalNodeKeepsLevelAndReplicationPort()
    {
        var config = new ClusteringConfig
        {
            BrokerId = 0,
            Host = "localhost",
            Port = 9092,
            ReplicationPort = 10092,
            RebalanceCheckIntervalSeconds = 5,
            // Configs routinely list ALL brokers including self — parsing this list must not
            // clobber the just-registered local node back to the KafkaWire default.
            ClusterNodes = "0:localhost:9092,1:localhost:9094",
        };
        var (controller, state) = NewController(config);

        await controller.StartAsync(CancellationToken.None);

        var local = state.GetBroker(0);
        Assert.NotNull(local);
        Assert.Equal(InterBrokerProtocolFeature.LocalMaxSupported, local!.InterBrokerProtocol);
        Assert.Equal(10092, local.ReplicationPort);

        // The config-discovered PEER stays at the safe KafkaWire default until it registers itself.
        var peer = state.GetBroker(1);
        Assert.NotNull(peer);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, peer!.InterBrokerProtocol);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task StartAsync_SingleBroker_FinalizesToLocalMaxSupported()
    {
        var config = new ClusteringConfig
        {
            BrokerId = 3,
            Host = "localhost",
            Port = 9092,
            ReplicationPort = 10092,
            RebalanceCheckIntervalSeconds = 5,
        };
        var (controller, state) = NewController(config);

        await controller.StartAsync(CancellationToken.None);

        Assert.Equal(InterBrokerProtocolFeature.LocalMaxSupported, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task StartAsync_PeerRegisteredBeforeConfigDiscovery_KeepsLevelAndReplicationPort()
    {
        var config = new ClusteringConfig
        {
            BrokerId = 0,
            Host = "localhost",
            Port = 9092,
            ReplicationPort = 10092,
            RebalanceCheckIntervalSeconds = 5,
            // The config lists peer 1 with a 3-part entry (replication port would be DERIVED as
            // 9094 + 1000 — a guess).
            ClusterNodes = "0:localhost:9092,1:localhost:9094",
        };
        var (controller, state) = NewController(config);

        // #72 Inc3 — startup-ordering race: peer 1 REGISTERED (native level, real replication port)
        // before this broker's config discovery ran. Discovery must be insert-only — the old
        // AddBroker overwrite reset the peer to KafkaWire/derived-port, dropping the finalized
        // level and re-introducing the #69 wrong-fetch-port failure.
        state.AddBroker(new BrokerNode
        {
            BrokerId = 1, Host = "peer-real", Port = 9095, ReplicationPort = 12345,
            InterBrokerProtocol = InterBrokerProtocolFeature.Native,
        });

        await controller.StartAsync(CancellationToken.None);

        var peer = state.GetBroker(1);
        Assert.NotNull(peer);
        Assert.Equal(InterBrokerProtocolFeature.Native, peer!.InterBrokerProtocol);
        Assert.Equal("peer-real", peer.Host);
        Assert.Equal(12345, peer.ReplicationPort);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }
}

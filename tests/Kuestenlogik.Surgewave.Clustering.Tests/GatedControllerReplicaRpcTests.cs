using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc5 — routing coverage for the control-plane transport gate: native client only when the
/// cluster-wide finalized <c>inter.broker.protocol</c> level (MIN over all registered brokers, Inc3)
/// is native; Kafka-wire fallback — or a logged drop when none exists — otherwise. The level is
/// re-read per call, so a mid-flight downgrade (old broker joins) reroutes immediately.
/// </summary>
public class GatedControllerReplicaRpcTests
{
    private sealed class RecordingRpc : IIsrChangeNotifier
    {
        public int IsrChangeCalls;

        public Task NotifyIsrChangedAsync(TopicPartition tp, int leaderId, int leaderEpoch, IReadOnlyList<int> isr, CancellationToken ct = default)
        { IsrChangeCalls++; return Task.CompletedTask; }
    }

    private static ClusterState ClusterWith(params short[] brokerLevels)
    {
        var state = new ClusterState();
        for (var i = 0; i < brokerLevels.Length; i++)
        {
            state.AddBroker(new BrokerNode
            {
                BrokerId = i,
                Host = "localhost",
                Port = 9092 + i,
                InterBrokerProtocol = brokerLevels[i],
            });
        }
        return state;
    }

    private static GatedControllerReplicaRpc Gate(ClusterState state, RecordingRpc native, RecordingRpc? fallback)
        => new(state, native, fallback, NullLogger<GatedControllerReplicaRpc>.Instance);

    [Fact]
    public async Task AllBrokersNative_RoutesToNativeClient()
    {
        var state = ClusterWith(InterBrokerProtocolFeature.Native, InterBrokerProtocolFeature.Native);
        var (native, fallback) = (new RecordingRpc(), new RecordingRpc());
        var gate = Gate(state, native, fallback);

        await gate.NotifyIsrChangedAsync(new TopicPartition { Topic = "t", Partition = 0 }, 0, 0, [0]);

        Assert.Equal(1, native.IsrChangeCalls);
        Assert.Equal(0, fallback.IsrChangeCalls);
    }

    [Fact]
    public async Task OneKafkaWireBroker_PinsToFallback()
    {
        // The safety anchor: a single old/incapable peer keeps the ISR report on the Kafka wire.
        var state = ClusterWith(InterBrokerProtocolFeature.Native, InterBrokerProtocolFeature.KafkaWire);
        var (native, fallback) = (new RecordingRpc(), new RecordingRpc());
        var gate = Gate(state, native, fallback);

        await gate.NotifyIsrChangedAsync(new TopicPartition { Topic = "t", Partition = 0 }, 0, 0, [0]);

        Assert.Equal(0, native.IsrChangeCalls);
        Assert.Equal(1, fallback.IsrChangeCalls);
    }

    [Fact]
    public async Task EmptyCluster_PinsToFallback()
    {
        var (native, fallback) = (new RecordingRpc(), new RecordingRpc());
        var gate = Gate(new ClusterState(), native, fallback);

        await gate.NotifyIsrChangedAsync(new TopicPartition { Topic = "t", Partition = 0 }, 0, 0, [0]);

        Assert.Equal(0, native.IsrChangeCalls);
        Assert.Equal(1, fallback.IsrChangeCalls);
    }

    [Fact]
    public async Task KafkaWirePinned_WithoutFallback_DropsWithoutThrowing()
    {
        // Native-only broker (no Kafka plugin) in a cluster still pinned to the Kafka wire:
        // best-effort drop, same as the pre-Inc5 behavior of a plugin-free broker.
        var state = ClusterWith(InterBrokerProtocolFeature.KafkaWire);
        var native = new RecordingRpc();
        var gate = Gate(state, native, fallback: null);

        await gate.NotifyIsrChangedAsync(new TopicPartition { Topic = "t", Partition = 0 }, 0, 0, [0]);

        Assert.Equal(0, native.IsrChangeCalls);
    }

    [Fact]
    public async Task OldBrokerJoiningMidFlight_ReroutesNextCallToFallback()
    {
        var state = ClusterWith(InterBrokerProtocolFeature.Native);
        var (native, fallback) = (new RecordingRpc(), new RecordingRpc());
        var gate = Gate(state, native, fallback);

        await gate.NotifyIsrChangedAsync(new TopicPartition { Topic = "t", Partition = 0 }, 0, 0, [0]);
        Assert.Equal(1, native.IsrChangeCalls);

        // An old broker registers → the finalized level drops → the very next call falls back.
        state.AddBroker(new BrokerNode { BrokerId = 9, Host = "localhost", Port = 9101, InterBrokerProtocol = InterBrokerProtocolFeature.KafkaWire });

        await gate.NotifyIsrChangedAsync(new TopicPartition { Topic = "t", Partition = 0 }, 0, 0, [0]);
        Assert.Equal(1, native.IsrChangeCalls);
        Assert.Equal(1, fallback.IsrChangeCalls);
    }
}

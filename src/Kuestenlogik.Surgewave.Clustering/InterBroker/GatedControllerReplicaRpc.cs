using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc5 — the transport gate for the controller→replica control plane. Every call re-reads
/// <see cref="ClusterState.FinalizedInterBrokerProtocol"/> (the MIN over all registered brokers, Inc3)
/// and routes to the <see cref="NativeControllerClient"/> when the whole cluster speaks
/// <see cref="InterBrokerProtocolFeature.Native"/>, else to the legacy Kafka-wire client. Because the
/// finalized level is the cluster minimum, selecting native here guarantees EVERY peer can receive
/// native frames; and because the level is re-read per send, an old broker joining mid-flight
/// immediately drops the cluster back to the Kafka wire (rolling-upgrade safe, both directions).
/// <para>
/// A broker without the Kafka plugin passes <c>null</c> as the fallback: while the cluster is pinned
/// to <see cref="InterBrokerProtocolFeature.KafkaWire"/> its pushes are dropped (logged) — the same
/// single-broker behavior such a deployment has today — and flow natively once the cluster finalizes
/// to native (plugin-free clustering completes with native registration, Inc6).
/// </para>
/// </summary>
public sealed partial class GatedControllerReplicaRpc : IControllerReplicaRpc
{
    /// <summary>
    /// DI service key under which the Kafka plugin registers its wire client
    /// (keyed <see cref="IControllerReplicaRpc"/>), so the broker host can compose it as this gate's
    /// fallback without an unkeyed registration clash.
    /// </summary>
    public const string WireFallbackServiceKey = "controller-replica-rpc:kafka-wire";

    private readonly ClusterState _clusterState;
    private readonly IControllerReplicaRpc _nativeClient;
    private readonly IControllerReplicaRpc? _kafkaWireFallback;
    private readonly ILogger<GatedControllerReplicaRpc> _logger;

    public GatedControllerReplicaRpc(
        ClusterState clusterState,
        IControllerReplicaRpc nativeClient,
        IControllerReplicaRpc? kafkaWireFallback,
        ILogger<GatedControllerReplicaRpc> logger)
    {
        _clusterState = clusterState;
        _nativeClient = nativeClient;
        _kafkaWireFallback = kafkaWireFallback;
        _logger = logger;
    }

    public Task SendLeaderAndIsrAsync(
        IEnumerable<(TopicPartition Tp, PartitionState State)> partitionChanges,
        CancellationToken ct = default)
        => Select()?.SendLeaderAndIsrAsync(partitionChanges, ct) ?? Task.CompletedTask;

    public Task SendUpdateMetadataAsync(
        IEnumerable<(TopicPartition Tp, PartitionState State)>? partitionStates = null,
        CancellationToken ct = default)
        => Select()?.SendUpdateMetadataAsync(partitionStates, ct) ?? Task.CompletedTask;

    public Task SendStopReplicaAsync(
        int brokerId,
        IEnumerable<(TopicPartition Tp, int LeaderEpoch, bool DeletePartition)> partitions,
        CancellationToken ct = default)
        => Select()?.SendStopReplicaAsync(brokerId, partitions, ct) ?? Task.CompletedTask;

    public Task NotifyIsrChangedAsync(
        TopicPartition tp,
        int leaderId,
        int leaderEpoch,
        IReadOnlyList<int> isr,
        CancellationToken ct = default)
        => Select()?.NotifyIsrChangedAsync(tp, leaderId, leaderEpoch, isr, ct) ?? Task.CompletedTask;

    private IControllerReplicaRpc? Select()
    {
        if (_clusterState.FinalizedInterBrokerProtocol >= InterBrokerProtocolFeature.Native)
            return _nativeClient;

        if (_kafkaWireFallback is null)
        {
            // Cluster is pinned to the Kafka wire but this broker cannot speak it (no Kafka plugin).
            // Best-effort control plane: drop, exactly like the pre-Inc5 native-only broker did.
            LogNoTransport();
            return null;
        }

        return _kafkaWireFallback;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Dropping controller push: cluster is finalized to the Kafka wire but no Kafka-wire client is available (native-only broker)")]
    private partial void LogNoTransport();
}

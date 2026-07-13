using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc7 — the transport gate for transaction-marker replication (mirrors
/// <see cref="GatedControllerReplicaRpc"/>). Each call re-reads
/// <see cref="ClusterState.FinalizedInterBrokerProtocol"/> and routes to the
/// <see cref="NativeTransactionMarkerReplicator"/> when the whole cluster speaks native, else to the
/// Kafka-wire replicator. Because the finalized level is the cluster MIN, selecting native guarantees
/// every follower can receive native marker frames; re-reading per call keeps a mid-flight old broker
/// safely on the Kafka wire.
/// </summary>
public sealed partial class GatedTransactionMarkerReplicator : ITransactionMarkerReplicator
{
    /// <summary>
    /// DI key under which the Kafka plugin registers its wire replicator as the fallback (keyed to
    /// avoid clashing with the host's unkeyed <see cref="ITransactionMarkerReplicator"/> gate).
    /// </summary>
    public const string WireFallbackServiceKey = "txn-marker-replicator:kafka-wire";

    private readonly ClusterState _clusterState;
    private readonly ITransactionMarkerReplicator _nativeReplicator;
    private readonly ITransactionMarkerReplicator? _kafkaWireFallback;
    private readonly ILogger<GatedTransactionMarkerReplicator> _logger;

    public GatedTransactionMarkerReplicator(
        ClusterState clusterState,
        ITransactionMarkerReplicator nativeReplicator,
        ITransactionMarkerReplicator? kafkaWireFallback,
        ILogger<GatedTransactionMarkerReplicator> logger)
    {
        _clusterState = clusterState;
        _nativeReplicator = nativeReplicator;
        _kafkaWireFallback = kafkaWireFallback;
        _logger = logger;
    }

    public Task<MarkerReplicationResult> ReplicateMarkersAsync(
        string transactionalId, long producerId, short producerEpoch,
        IReadOnlyList<TopicPartition> partitions, bool commit, int coordinatorEpoch, CancellationToken cancellationToken)
    {
        if (_clusterState.FinalizedInterBrokerProtocol >= InterBrokerProtocolFeature.Native)
            return _nativeReplicator.ReplicateMarkersAsync(transactionalId, producerId, producerEpoch, partitions, commit, coordinatorEpoch, cancellationToken);

        if (_kafkaWireFallback is not null)
            return _kafkaWireFallback.ReplicateMarkersAsync(transactionalId, producerId, producerEpoch, partitions, commit, coordinatorEpoch, cancellationToken);

        // Cluster pinned to the Kafka wire but this broker cannot speak it (native-only, no plugin).
        // Report failure so the coordinator does not treat the markers as durably replicated.
        LogNoTransport(transactionalId);
        return Task.FromResult(new MarkerReplicationResult { IsSuccess = false });
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Cannot replicate txn markers for {TransactionalId}: cluster is finalized to the Kafka wire but no Kafka-wire replicator is available (native-only broker)")]
    private partial void LogNoTransport(string transactionalId);
}

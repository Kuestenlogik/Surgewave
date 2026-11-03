using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

public sealed partial class ClusterController
{
    private int _ensuringInternalTopics;

    /// <summary>
    /// Creates <c>__consumer_offsets</c> once the cluster can carry it (#204). Called when this broker becomes
    /// controller and after every broker registration, so the topic appears as soon as enough brokers are known
    /// for its replication factor; concurrent calls collapse into one. Compacted, as Kafka's.
    /// </summary>
    /// <remarks>
    /// Created up front rather than on the first FindCoordinator as Kafka does: a broker that is not the
    /// controller has no way yet to ask the controller to create a topic, and a FindCoordinator that reached the
    /// controller only by chance would leave groups waiting on luck.
    /// </remarks>
    private async Task EnsureInternalTopicsAsync(CancellationToken ct)
    {
        if (!_isController)
            return;
        var existing = _clusterState.GetTopic(CoordinatorPartitioner.GroupTopic);
        if (existing is not null && UnassignedPartitions(existing).Count == 0)
            return;
        if (Interlocked.Exchange(ref _ensuringInternalTopics, 1) == 1)
            return;

        try
        {
            if (existing is not null)
            {
                // A controller that lost its leadership between TopicCreated and the partition assignments left the
                // topic without them; every group mapped to such a partition would have no coordinator.
                var missing = UnassignedPartitions(existing);
                LogInternalTopicCompleting(existing.Name, missing.Count);
                if (!await AssignPartitionsViaRaftAsync(existing.Name, existing.TopicId, missing, existing.ReplicationFactor, ct)
                        .ConfigureAwait(false))
                    LogInternalTopicNotCreated(existing.Name);
                return;
            }

            var replicationFactor = (short)Math.Min(_config.OffsetsTopicReplicationFactor, ConfiguredClusterSize());
            if (_clusterState.Brokers.Count < replicationFactor)
            {
                LogInternalTopicWaitingForBrokers(CoordinatorPartitioner.GroupTopic, _clusterState.Brokers.Count, replicationFactor);
                return;
            }

            var created = await CreateTopicAsync(
                CoordinatorPartitioner.GroupTopic,
                _config.OffsetsTopicNumPartitions,
                replicationFactor,
                new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
                ct).ConfigureAwait(false);
            if (!created)
                LogInternalTopicNotCreated(CoordinatorPartitioner.GroupTopic);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutting down; the next controller creates it.
        }
        finally
        {
            Volatile.Write(ref _ensuringInternalTopics, 0);
        }
    }

    // The partitions of a topic the metadata log has no replica assignment for.
    private List<int> UnassignedPartitions(Core.Models.TopicMetadata topic) =>
        [.. Enumerable.Range(0, topic.PartitionCount).Where(p =>
            _clusterState.GetPartitionState(new Core.Models.TopicPartition { Topic = topic.Name, Partition = p }) is not { Replicas.Count: > 0 })];

    /// <summary>
    /// The brokers this cluster is configured with: this one and every other id in ClusterNodes. A replication
    /// factor above it could never be met, so a broker on its own gets 1.
    /// </summary>
    private int ConfiguredClusterSize()
    {
        var ids = new HashSet<int> { _config.BrokerId };
        foreach (var node in _config.ClusterNodes.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var id = node.Trim().Split(':')[0];
            if (int.TryParse(id, out var brokerId))
                ids.Add(brokerId);
        }
        return ids.Count;
    }

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Internal topic {Topic} waits for brokers: {Known} known, replication factor {ReplicationFactor}")]
    private partial void LogInternalTopicWaitingForBrokers(string topic, int known, short replicationFactor);

    [LoggerMessage(Level = LogLevel.Information, Message = "Internal topic {Topic} exists without an assignment for {Missing} partition(s); assigning them")]
    private partial void LogInternalTopicCompleting(string topic, int missing);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Internal topic {Topic} could not be created; retried on the next broker registration")]
    private partial void LogInternalTopicNotCreated(string topic);
}

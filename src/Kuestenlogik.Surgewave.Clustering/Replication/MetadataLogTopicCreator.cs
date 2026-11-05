using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Creates a topic through the controller into the metadata log (#214) — directly on the controller, through it from
/// any other broker (#207) — and completes once the topic has reached this broker: its partitions are assigned in the
/// cluster state, and those this broker is a replica of are in its log manager. A topic created this way is on every
/// broker and survives a restart, as the metadata log is replayed at the start.
/// </summary>
public sealed class MetadataLogTopicCreator(
    IClusterTopicCreator controller, ClusterState clusterState, LogManager logManager, int brokerId) : ITopicCreator
{
    /// <inheritdoc />
    public async Task<TopicMetadata> CreateTopicAsync(
        string topicName,
        int partitionCount,
        short replicationFactor = 1,
        Dictionary<string, string>? config = null,
        CancellationToken cancellationToken = default)
    {
        if (clusterState.GetTopic(topicName) is not null || logManager.GetTopicMetadata(topicName) is not null)
            throw new InvalidOperationException($"Topic '{topicName}' already exists");

        if (!await controller.CreateTopicAsync(topicName, partitionCount, replicationFactor, config, cancellationToken).ConfigureAwait(false))
            throw new TopicNotCreatedException(topicName);

        // The controller committed it; this broker applies the metadata log on its own time. Each signal is taken before
        // the look, so a change between the look and the wait is not missed.
        while (true)
        {
            var clusterChanged = clusterState.TopicsChanged;
            var localChanged = logManager.TopicsChanged;
            if (Arrived(topicName) is { } topic)
                return topic;
            await Task.WhenAny(clusterChanged, localChanged).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    // The topic as this broker knows it once every partition is assigned and those it is a replica of are local; null
    // before.
    private TopicMetadata? Arrived(string topicName)
    {
        if (clusterState.GetTopic(topicName) is not { } known)
            return null;

        var local = logManager.GetTopicMetadata(topicName);
        for (var p = 0; p < known.PartitionCount; p++)
        {
            if (clusterState.GetPartitionState(new TopicPartition { Topic = topicName, Partition = p }) is not { } state)
                return null;
            if (state.Replicas.Contains(brokerId) && (local is null || local.PartitionCount <= p))
                return null;
        }
        return local ?? known;
    }
}

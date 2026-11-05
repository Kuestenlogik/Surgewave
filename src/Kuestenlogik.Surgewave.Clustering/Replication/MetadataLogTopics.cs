using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Creates topics and changes their configs through the controller in the metadata log — directly on the controller,
/// through it from any other broker (#207) — and completes once the change has reached this broker. A topic created
/// this way is on every broker and survives a restart (#214); a config changed this way reaches every broker hosting
/// the topic's replicas, and a broker that takes a partition over runs it with the topic's config (#119).
/// </summary>
public sealed class MetadataLogTopics(
    IClusterTopicCreator creator, IClusterTopicConfigWriter configWriter, ClusterState clusterState, LogManager logManager, int brokerId)
    : ITopicCreator, ITopicConfigWriter
{
    /// <inheritdoc />
    public async Task<TopicMetadata> CreateTopicAsync(
        string topicName,
        int partitionCount,
        short replicationFactor,
        Dictionary<string, string>? config,
        CancellationToken cancellationToken)
    {
        if (clusterState.GetTopic(topicName) is not null || logManager.GetTopicMetadata(topicName) is not null)
            throw new InvalidOperationException($"Topic '{topicName}' already exists");

        if (!await creator.CreateTopicAsync(topicName, partitionCount, replicationFactor, config, cancellationToken).ConfigureAwait(false))
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

    /// <inheritdoc />
    public async Task<bool> ChangeTopicConfigAsync(
        string topicName, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken cancellationToken)
    {
        if (clusterState.GetTopic(topicName) is null)
            return false;

        if (!await configWriter.ChangeTopicConfigAsync(topicName, set, removed, cancellationToken).ConfigureAwait(false))
            throw new TopicConfigNotChangedException(topicName);

        while (true)
        {
            var clusterChanged = clusterState.TopicsChanged;
            var localChanged = logManager.TopicsChanged;
            if (clusterState.GetTopic(topicName) is not { } known)
                return false; // deleted on the way
            var local = logManager.GetTopicMetadata(topicName);
            if (Holds(known.Config, set, removed) && (local is null || Holds(local.Config, set, removed)))
                return true;
            await Task.WhenAny(clusterChanged, localChanged).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    // Whether a config has the values set and none of those removed.
    private static bool Holds(IReadOnlyDictionary<string, string> config, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed) =>
        set.All(kv => config.TryGetValue(kv.Key, out var value) && value == kv.Value) && removed.All(k => !config.ContainsKey(k));

    // The topic as this broker knows it once every partition is assigned and those it is a replica of are local; null
    // before.
    private TopicMetadata? Arrived(string topicName)
    {
        if (clusterState.GetTopic(topicName) is not { } known)
            return null;

        var local = logManager.GetTopicMetadata(topicName);
        for (var p = 0; p < known.PartitionCount; p++)
        {
            // Read under the lock the assignment is written under: a partition is created before its replicas are
            // filled in, and one without replicas yet is not assigned.
            var replicas = clusterState.ReplicasOf(new TopicPartition { Topic = topicName, Partition = p });
            if (replicas.Count == 0)
                return null;
            if (replicas.Contains(brokerId) && (local is null || local.PartitionCount <= p))
                return null;
        }
        return local ?? known;
    }
}

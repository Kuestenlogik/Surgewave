using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// In a cluster, a group's coordinator is the leader of its partition of <c>__consumer_offsets</c> (#204), and a
/// transactional id's the leader of its partition of <c>__transaction_state</c> (#197), as in Kafka. Every broker
/// answers the same, so the clients of one group or one transactional id meet at one coordinator whichever broker
/// they bootstrapped from.
/// </summary>
public sealed class ClusterCoordinatorLocator(ClusterState clusterState) : ICoordinatorLocator
{
    /// <inheritdoc />
    public CoordinatorLocation Locate(CoordinatorKind kind, string key)
    {
        var leader = LeaderOf(kind, key);
        return leader is { } broker
            ? new CoordinatorLocation(CoordinatorLookupStatus.Ok, broker.BrokerId, broker.Host, broker.Port)
            : CoordinatorLocation.NotAvailable;
    }

    /// <inheritdoc />
    public bool IsLocal(CoordinatorKind kind, string key) =>
        LeaderOf(kind, key)?.BrokerId == clusterState.LocalBrokerId;

    private BrokerNode? LeaderOf(CoordinatorKind kind, string key)
    {
        var topicName = kind switch
        {
            CoordinatorKind.Transaction => CoordinatorPartitioner.TransactionTopic,
            CoordinatorKind.Share => CoordinatorPartitioner.ShareStateTopic,
            _ => CoordinatorPartitioner.GroupTopic,
        };

        // The topic's actual partition count, not the configured one: a topic created before a config change keeps
        // its partitions, and hashing over a different count would send a key to a partition it never used.
        var topic = clusterState.GetTopic(topicName);
        if (topic is null || topic.PartitionCount < 1)
            return null;

        var tp = new TopicPartition
        {
            Topic = topicName,
            Partition = CoordinatorPartitioner.PartitionFor(key, topic.PartitionCount),
        };
        var leaderId = clusterState.GetPartitionState(tp)?.LeaderBrokerId ?? -1;
        return leaderId < 0 ? null : clusterState.GetBroker(leaderId);
    }
}

namespace Kuestenlogik.Surgewave.Broker.Native.Assignors;

/// <summary>
/// Partition assignment strategies for consumer groups.
/// These implement the client-side partition assignment algorithms.
/// </summary>
public interface IPartitionAssignor
{
    /// <summary>
    /// Name of the assignment strategy
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Assign partitions to members
    /// </summary>
    Dictionary<string, List<AssignedPartition>> Assign(
        List<string> topics,
        Dictionary<string, int> topicPartitionCounts,
        List<MemberSubscription> members);
}

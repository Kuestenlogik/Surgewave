namespace Kuestenlogik.Surgewave.Broker.Native.Assignors;

/// <summary>
/// Cooperative sticky assignor - like sticky but uses cooperative rebalancing.
/// Members only revoke partitions that are being reassigned.
/// </summary>
public sealed class CooperativeStickyAssignor : IPartitionAssignor
{
    private readonly StickyAssignor _stickyAssignor = new();

    public string Name => "cooperative-sticky";

    public Dictionary<string, List<AssignedPartition>> Assign(
        List<string> topics,
        Dictionary<string, int> topicPartitionCounts,
        List<MemberSubscription> members)
    {
        // Use sticky assignment as the base
        return _stickyAssignor.Assign(topics, topicPartitionCounts, members);
    }
}

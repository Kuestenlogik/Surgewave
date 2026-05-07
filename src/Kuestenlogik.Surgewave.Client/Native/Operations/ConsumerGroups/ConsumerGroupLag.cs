namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Consumer group lag information.
/// </summary>
public sealed record ConsumerGroupLag(
    string GroupId,
    string State,
    long TotalLag,
    int PartitionCount,
    int MemberCount,
    IReadOnlyList<TopicLag> Topics);

/// <summary>
/// Lag information for a topic within a consumer group.
/// </summary>
public sealed record TopicLag(
    string Topic,
    long TotalLag,
    IReadOnlyList<PartitionLag> Partitions);

/// <summary>
/// Lag information for a partition.
/// </summary>
public sealed record PartitionLag(
    int Partition,
    long CommittedOffset,
    long HighWatermark,
    long Lag,
    long LogStartOffset);

/// <summary>
/// Summary of lag across all consumer groups.
/// </summary>
public sealed record LagSummaryResult(
    int GroupCount,
    int GroupsWithHighLag,
    long TotalLag,
    long MaxLag,
    string? MaxLagGroup,
    IReadOnlyList<LagSummaryGroup> Groups);

/// <summary>
/// Summary lag info for a consumer group.
/// </summary>
public sealed record LagSummaryGroup(
    string GroupId,
    string State,
    long TotalLag,
    int PartitionCount,
    int MemberCount);

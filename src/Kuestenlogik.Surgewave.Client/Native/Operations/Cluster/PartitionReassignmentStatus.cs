namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Status of a partition reassignment.
/// </summary>
public record PartitionReassignmentStatus(
    string Topic,
    int Partition,
    ReassignmentStatusCode Status,
    int ProgressPercent,
    List<int> OriginalReplicas,
    List<int> TargetReplicas);

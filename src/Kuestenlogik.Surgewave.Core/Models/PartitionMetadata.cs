namespace Kuestenlogik.Surgewave.Core.Models;

/// <summary>
/// Partition metadata
/// </summary>
public sealed record PartitionMetadata(
    int PartitionId,
    int Leader,
    List<int> Replicas,
    List<int> InSyncReplicas);

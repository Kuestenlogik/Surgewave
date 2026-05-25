namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Result of transferring leadership for all partitions led by a broker.
/// </summary>
public sealed record TransferResult(
    int TotalPartitions,
    int Transferred,
    int Failed,
    IReadOnlyList<string> FailedPartitions);

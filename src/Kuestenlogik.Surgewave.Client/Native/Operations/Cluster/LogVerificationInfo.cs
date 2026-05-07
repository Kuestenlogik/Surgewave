namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Result of log integrity verification.
/// </summary>
public sealed record LogVerificationInfo
{
    /// <summary>
    /// Total number of batches checked.
    /// </summary>
    public int BatchesChecked { get; init; }

    /// <summary>
    /// Number of corrupted batches found.
    /// </summary>
    public int CorruptedBatches { get; init; }

    /// <summary>
    /// Total bytes checked.
    /// </summary>
    public long BytesChecked { get; init; }

    /// <summary>
    /// Total bytes in corrupted batches.
    /// </summary>
    public long CorruptedBytes { get; init; }

    /// <summary>
    /// Total number of partitions checked.
    /// </summary>
    public int PartitionsChecked { get; init; }

    /// <summary>
    /// Time taken to perform the verification.
    /// </summary>
    public TimeSpan Duration { get; init; }

    /// <summary>
    /// Topics that were verified.
    /// </summary>
    public IReadOnlyList<string> TopicsVerified { get; init; } = [];

    /// <summary>
    /// Details of each corrupted batch found.
    /// </summary>
    public IReadOnlyList<CorruptedBatchDetail> CorruptedBatchDetails { get; init; } = [];

    /// <summary>
    /// Whether the verification passed (no corruption found).
    /// </summary>
    public bool IsValid => CorruptedBatches == 0;
}

/// <summary>
/// Information about a corrupted batch.
/// </summary>
public sealed record CorruptedBatchDetail(
    string Topic,
    int Partition,
    long BaseOffset,
    uint ExpectedCrc,
    uint ActualCrc,
    int BatchLength);

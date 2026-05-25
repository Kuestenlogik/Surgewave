namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Result of log integrity verification.
/// </summary>
public sealed class LogVerificationResult
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
    /// Details of each corrupted batch found.
    /// </summary>
    public List<CorruptedBatchInfo> CorruptedBatchDetails { get; init; } = [];

    /// <summary>
    /// Whether the verification passed (no corruption found).
    /// </summary>
    public bool IsValid => CorruptedBatches == 0;

    /// <summary>
    /// Topics that were verified.
    /// </summary>
    public List<string> TopicsVerified { get; init; } = [];

    /// <summary>
    /// Total number of partitions checked.
    /// </summary>
    public int PartitionsChecked { get; init; }

    /// <summary>
    /// Time taken to perform the verification.
    /// </summary>
    public TimeSpan Duration { get; init; }
}

/// <summary>
/// Verification options for log integrity checks.
/// </summary>
public sealed class LogVerificationOptions
{
    /// <summary>
    /// Specific topic to verify. If null, verifies all topics.
    /// </summary>
    public string? Topic { get; init; }

    /// <summary>
    /// Specific partition to verify. If null, verifies all partitions.
    /// Requires Topic to be set.
    /// </summary>
    public int? Partition { get; init; }

    /// <summary>
    /// Stop verification after finding this many corrupted batches.
    /// 0 = no limit.
    /// </summary>
    public int MaxCorruptedBatches { get; init; } = 0;

    /// <summary>
    /// Include detailed information for each corrupted batch.
    /// </summary>
    public bool IncludeDetails { get; init; } = true;
}

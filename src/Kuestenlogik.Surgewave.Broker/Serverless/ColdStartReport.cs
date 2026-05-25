namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Report produced after a cold start optimization pass,
/// containing timing and resource pre-fetch statistics.
/// </summary>
public sealed record ColdStartReport
{
    /// <summary>
    /// Total wall-clock duration of the cold start phase.
    /// </summary>
    public required TimeSpan TotalDuration { get; init; }

    /// <summary>
    /// Number of partition metadata entries loaded from object storage.
    /// </summary>
    public required int PartitionsLoaded { get; init; }

    /// <summary>
    /// Number of hot partitions whose read caches were pre-warmed.
    /// </summary>
    public required int PartitionsPreWarmed { get; init; }

    /// <summary>
    /// Total bytes pre-fetched from remote storage into local cache.
    /// </summary>
    public required long BytesPreFetched { get; init; }

    /// <summary>
    /// Lifecycle state at the end of the cold start process.
    /// </summary>
    public required ServerlessLifecycleState FinalState { get; init; }
}

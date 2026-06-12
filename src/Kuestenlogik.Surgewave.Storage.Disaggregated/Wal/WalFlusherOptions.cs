namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;

/// <summary>
/// Operator-tweakable knobs for the <see cref="WalFlusher"/>. The
/// defaults target the AutoMQ-style "WAL trimmed within seconds"
/// rhythm from ADR-014 — operators who want WarpStream-style
/// per-batch flushing should use <c>storage.mode=disaggregated-
/// stateless</c> instead (lands in P3).
/// </summary>
public sealed record WalFlusherOptions
{
    /// <summary>
    /// How long the flusher sleeps between scans of each partition for
    /// new sealed segments. Smaller = WAL shrinks faster, S3 PUT costs
    /// rise. Default: 5 s.
    /// </summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of sealed segments to flush per partition per scan.
    /// Bounds the burst of S3 PUTs when a quiet partition suddenly has a
    /// lot of catch-up to do. Default: 16.
    /// </summary>
    public int MaxSegmentsPerScan { get; init; } = 16;
}

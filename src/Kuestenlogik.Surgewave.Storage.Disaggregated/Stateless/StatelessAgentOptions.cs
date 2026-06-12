namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Stateless;

/// <summary>
/// Operator-tweakable knobs for the WarpStream-style
/// <see cref="StatelessAgent"/>. The defaults target the
/// "P99 produce latency ~400-600 ms, cost-cheap" sweet spot from
/// ADR-014. Operators who care more about latency than cost should
/// pick <c>storage.mode=disaggregated-wal</c> instead.
/// </summary>
public sealed record StatelessAgentOptions
{
    /// <summary>
    /// Maximum bytes the per-partition RAM buffer holds before forcing a
    /// flush. Larger = fewer S3 PUTs (cheaper) + higher tail-latency on
    /// the records waiting at the back of the buffer. Default: 4 MiB —
    /// roughly the upper end of what fits in a single S3 PUT before
    /// multi-part-upload kicks in.
    /// </summary>
    public long MaxBufferBytes { get; init; } = 4L * 1024 * 1024;

    /// <summary>
    /// Maximum age of the oldest record in the buffer before forcing a
    /// flush. The first record's await time roughly equals this value
    /// under sparse load. Default: 500 ms (WarpStream's stated upper
    /// bound for produce-P99).
    /// </summary>
    public TimeSpan MaxBufferAge { get; init; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// How often the background loop checks for age-triggered flushes.
    /// Should be small relative to <see cref="MaxBufferAge"/>; the
    /// effective P99 = MaxBufferAge + AgePollInterval/2 in the worst
    /// case. Default: 50 ms.
    /// </summary>
    public TimeSpan AgePollInterval { get; init; } = TimeSpan.FromMilliseconds(50);
}

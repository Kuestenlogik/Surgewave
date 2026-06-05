namespace Kuestenlogik.Surgewave.Broker.AdaptiveCompression;

/// <summary>
/// Configuration for the adaptive-compression background service that resolves
/// <c>compression.type=auto</c> topic configs to a concrete codec by sampling
/// the topic's recent record batches and scoring per
/// <see cref="Kuestenlogik.Surgewave.Core.Util.AdaptiveCompressionSampler"/>.
/// </summary>
public sealed class AdaptiveCompressionConfig
{
    /// <summary>Configuration section name (<c>Surgewave:AdaptiveCompression</c>).</summary>
    public const string SectionName = "Surgewave:AdaptiveCompression";

    /// <summary>The literal topic-config value that opts a topic into adaptive
    /// compression. Kept as a constant so the broker and tests stay in lockstep.</summary>
    public const string AutoMarker = "auto";

    /// <summary>Whether the background service runs at all.</summary>
    public bool Enabled { get; set; }

    /// <summary>Seconds between scan cycles. Each cycle enumerates every topic
    /// with <c>compression.type=auto</c>, reads up to
    /// <see cref="MaxScanBytesPerPartition"/> from each partition, and feeds the
    /// uncompressed records into that topic's sampler.</summary>
    public int ScanIntervalSeconds { get; set; } = 30;

    /// <summary>Maximum bytes to read per partition per scan cycle. Defaults to
    /// 1 MiB — enough to feed a meaningful sample without dominating disk I/O on
    /// very large topics.</summary>
    public int MaxScanBytesPerPartition { get; set; } = 1024 * 1024;

    /// <summary>Sample-every-Nth-record knob forwarded to the per-topic
    /// <see cref="Kuestenlogik.Surgewave.Core.Util.AdaptiveCompressionSampler"/>.
    /// </summary>
    public int SampleEveryNthRecord { get; set; } = 100;

    /// <summary>Minimum sweep count before a decision is written back. Forwarded
    /// to the per-topic sampler.</summary>
    public int MinSampleCount { get; set; } = 50;
}

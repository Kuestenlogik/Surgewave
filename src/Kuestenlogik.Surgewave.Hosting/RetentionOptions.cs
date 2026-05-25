namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Log retention configuration.
/// </summary>
public sealed class RetentionOptions
{
    /// <summary>
    /// Retention period in hours. -1 for infinite.
    /// Default: 168 (7 days).
    /// </summary>
    public int Hours { get; set; } = 168;

    /// <summary>
    /// Maximum log size in bytes. -1 for unlimited.
    /// Default: -1.
    /// </summary>
    public long Bytes { get; set; } = -1;

    /// <summary>
    /// Log segment size in bytes.
    /// Default: 1GB.
    /// </summary>
    public long SegmentBytes { get; set; } = 1024 * 1024 * 1024;
}

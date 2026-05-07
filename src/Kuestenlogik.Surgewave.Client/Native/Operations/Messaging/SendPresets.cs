namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Configuration preset for send operations.
/// </summary>
public sealed class SendPreset
{
    /// <summary>
    /// Compression type for messages.
    /// </summary>
    public CompressionType Compression { get; set; } = CompressionType.None;

    /// <summary>
    /// Compression level (codec-specific).
    /// </summary>
    public int CompressionLevel { get; set; } = -1;

    /// <summary>
    /// Partition selection strategy.
    /// </summary>
    public IPartitionStrategy? PartitionStrategy { get; set; }

    /// <summary>
    /// Default timeout for operations.
    /// </summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>
    /// Number of retry attempts.
    /// </summary>
    public int RetryAttempts { get; set; }

    /// <summary>
    /// Retry backoff interval.
    /// </summary>
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Whether to use exponential backoff.
    /// </summary>
    public bool ExponentialBackoff { get; set; } = true;

    /// <summary>
    /// Batch size for sticky partitioner.
    /// </summary>
    public int StickyBatchSize { get; set; } = 100;

    /// <summary>
    /// Linger time before sending batch.
    /// </summary>
    public TimeSpan LingerTime { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Maximum batch size in messages.
    /// </summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>
    /// Default headers to include with all messages.
    /// </summary>
    public Dictionary<string, byte[]>? DefaultHeaders { get; set; }

    // ═══════════════════════════════════════════════════════════════
    // Built-in presets
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// Default preset with no special configuration.
    /// </summary>
    public static SendPreset Default { get; } = new();

    /// <summary>
    /// High throughput preset optimized for maximum messages/sec.
    /// </summary>
    public static SendPreset HighThroughput { get; } = new()
    {
        Compression = CompressionType.Lz4,
        PartitionStrategy = Partitioner.Sticky,
        StickyBatchSize = 1000,
        LingerTime = TimeSpan.FromMilliseconds(5),
        MaxBatchSize = 10000
    };

    /// <summary>
    /// Low latency preset optimized for minimal delay.
    /// </summary>
    public static SendPreset LowLatency { get; } = new()
    {
        Compression = CompressionType.None,
        LingerTime = TimeSpan.Zero,
        MaxBatchSize = 1,
        Timeout = TimeSpan.FromSeconds(5),
        RetryAttempts = 1
    };

    /// <summary>
    /// Reliable preset with retries and compression.
    /// </summary>
    public static SendPreset Reliable { get; } = new()
    {
        Compression = CompressionType.Zstd,
        RetryAttempts = 5,
        RetryBackoff = TimeSpan.FromMilliseconds(200),
        ExponentialBackoff = true,
        Timeout = TimeSpan.FromSeconds(30)
    };

    /// <summary>
    /// High compression preset for storage optimization.
    /// </summary>
    public static SendPreset HighCompression { get; } = new()
    {
        Compression = CompressionType.Zstd,
        CompressionLevel = 9
    };
}

/// <summary>
/// Compression types for send operations.
/// </summary>
public enum CompressionType
{
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4
}

/// <summary>
/// Builder for creating custom presets.
/// </summary>
public sealed class SendPresetBuilder
{
    private readonly SendPreset _preset = new();

    /// <summary>
    /// Set compression type.
    /// </summary>
    public SendPresetBuilder WithCompression(CompressionType compression)
    {
        _preset.Compression = compression;
        return this;
    }

    /// <summary>
    /// Set compression level.
    /// </summary>
    public SendPresetBuilder WithCompressionLevel(int level)
    {
        _preset.CompressionLevel = level;
        return this;
    }

    /// <summary>
    /// Set partition strategy.
    /// </summary>
    public SendPresetBuilder WithPartitionStrategy(IPartitionStrategy strategy)
    {
        _preset.PartitionStrategy = strategy;
        return this;
    }

    /// <summary>
    /// Set timeout.
    /// </summary>
    public SendPresetBuilder WithTimeout(TimeSpan timeout)
    {
        _preset.Timeout = timeout;
        return this;
    }

    /// <summary>
    /// Set retry configuration.
    /// </summary>
    public SendPresetBuilder WithRetry(int attempts, TimeSpan? backoff = null, bool exponential = true)
    {
        _preset.RetryAttempts = attempts;
        if (backoff.HasValue) _preset.RetryBackoff = backoff.Value;
        _preset.ExponentialBackoff = exponential;
        return this;
    }

    /// <summary>
    /// Set batch size for sticky partitioner.
    /// </summary>
    public SendPresetBuilder WithStickyBatchSize(int size)
    {
        _preset.StickyBatchSize = size;
        return this;
    }

    /// <summary>
    /// Set linger time before sending.
    /// </summary>
    public SendPresetBuilder WithLingerTime(TimeSpan linger)
    {
        _preset.LingerTime = linger;
        return this;
    }

    /// <summary>
    /// Set maximum batch size.
    /// </summary>
    public SendPresetBuilder WithMaxBatchSize(int size)
    {
        _preset.MaxBatchSize = size;
        return this;
    }

    /// <summary>
    /// Set default headers.
    /// </summary>
    public SendPresetBuilder WithDefaultHeaders(Dictionary<string, byte[]> headers)
    {
        _preset.DefaultHeaders = headers;
        return this;
    }

    /// <summary>
    /// Build the preset.
    /// </summary>
    public SendPreset Build() => _preset;
}

/// <summary>
/// Extension methods for presets.
/// </summary>
public static class PresetExtensions
{
    /// <summary>
    /// Create a custom preset builder.
    /// </summary>
    public static SendPresetBuilder CreatePreset(this SurgewaveMessagingOperations _)
        => new();
}

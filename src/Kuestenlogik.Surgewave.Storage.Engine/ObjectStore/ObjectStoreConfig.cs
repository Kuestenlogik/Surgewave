using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Configuration for the object-store-backed storage engine.
/// Controls write buffer sizes, read cache limits, and flush behavior.
/// </summary>
public sealed class ObjectStoreConfig : IValidatableConfig
{
    /// <summary>
    /// Max local write buffer size before flush to remote storage (default 64MB).
    /// Larger buffers reduce upload frequency but increase memory usage and data-at-risk window.
    /// </summary>
    [Range(1024, long.MaxValue)]
    public long WriteBufferSizeBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Local read cache size limit (default 512MB).
    /// Controls maximum total bytes cached locally from remote segments.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long ReadCacheSizeBytes { get; init; } = 512 * 1024 * 1024;

    /// <summary>
    /// Local cache directory for downloaded segments.
    /// Used by the LRU read cache for on-disk segment caching.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string CacheDirectory { get; init; } = "./zero-disk-cache";

    /// <summary>
    /// Flush interval even if the write buffer is not full (default 30s).
    /// Ensures bounded data-at-risk window regardless of write volume.
    /// </summary>
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Default maximum segment size in bytes (default 1GB).
    /// </summary>
    [Range(1024, long.MaxValue)]
    public long DefaultMaxSegmentSize { get; init; } = 1024L * 1024 * 1024;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (FlushInterval <= TimeSpan.Zero)
            errors.Add($"{nameof(FlushInterval)}: must be positive.");

        if (DefaultMaxSegmentSize < WriteBufferSizeBytes)
            errors.Add($"{nameof(DefaultMaxSegmentSize)} ({DefaultMaxSegmentSize}) must be >= " +
                       $"{nameof(WriteBufferSizeBytes)} ({WriteBufferSizeBytes}).");

        return errors;
    }
}

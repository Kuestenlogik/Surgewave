namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Tiered storage configuration options.
/// </summary>
public sealed class TieredStorageOptions
{
    /// <summary>
    /// Enable tiered storage.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Storage provider: "local", "s3", "azure", "gcp".
    /// </summary>
    public string Provider { get; set; } = "local";

    /// <summary>
    /// Local cache directory for fetched remote segments.
    /// </summary>
    public string? CachePath { get; set; }

    /// <summary>
    /// Local cache size limit in bytes.
    /// Default: 1GB.
    /// </summary>
    public long CacheSizeBytes { get; set; } = 1024 * 1024 * 1024;

    /// <summary>
    /// Hours to keep segments locally before tiering.
    /// Default: 24.
    /// </summary>
    public int LocalRetentionHours { get; set; } = 24;

    /// <summary>
    /// S3-specific configuration.
    /// </summary>
    public S3Options? S3 { get; set; }

    /// <summary>
    /// Azure Blob Storage configuration.
    /// </summary>
    public AzureOptions? Azure { get; set; }

    /// <summary>
    /// Google Cloud Storage configuration.
    /// </summary>
    public GcpOptions? Gcp { get; set; }
}

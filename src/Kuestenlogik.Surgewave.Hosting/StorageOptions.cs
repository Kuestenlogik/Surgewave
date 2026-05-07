namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Storage-specific configuration options.
/// </summary>
public sealed class StorageOptions
{
    /// <summary>
    /// Enable memory-mapped I/O for file storage.
    /// Default: true (improves read performance).
    /// </summary>
    public bool UseMmap { get; set; } = true;

    /// <summary>
    /// Arrow compression codec: "zstd", "lz4", "snappy", "none".
    /// Default: "zstd".
    /// </summary>
    public string ArrowCompression { get; set; } = "zstd";

    /// <summary>
    /// Arrow compression level (1-22 for Zstd).
    /// Default: 3.
    /// </summary>
    public int ArrowCompressionLevel { get; set; } = 3;

    /// <summary>
    /// Arrow flush threshold in bytes.
    /// Default: 16MB.
    /// </summary>
    public long ArrowFlushBytes { get; set; } = 16 * 1024 * 1024;
}

using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Configuration options for the Surgewave broker.
/// Can be configured via appsettings.json or code.
/// </summary>
/// <example>
/// appsettings.json:
/// <code>
/// {
///   "Surgewave": {
///     "Port": 9092,
///     "NativePort": 9093,
///     "DataDirectory": "./data",
///     "Storage": "ZeroCopyWal",
///     "AutoCreateTopics": true,
///     "Topics": {
///       "DefaultPartitions": 3,
///       "DefaultReplicationFactor": 1
///     }
///   }
/// }
/// </code>
/// </example>
public sealed class SurgewaveOptions
{
    /// <summary>
    /// Default configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave";

    // ==================== Network ====================

    /// <summary>
    /// Host address to bind to. Default: "localhost".
    /// Use "0.0.0.0" or "::" to bind to all interfaces.
    /// </summary>
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Kafka protocol port. Default: 9092.
    /// Use 0 for automatic port assignment.
    /// </summary>
    public int Port { get; set; } = 9092;

    /// <summary>
    /// Native Surgewave protocol port. Default: 9093.
    /// Use 0 for automatic port assignment, -1 to disable.
    /// </summary>
    public int NativePort { get; set; } = 9093;

    /// <summary>
    /// Broker ID. Default: 0.
    /// Must be unique within a cluster.
    /// </summary>
    public int BrokerId { get; set; } = 0;

    // ==================== Storage ====================

    /// <summary>
    /// Data directory for logs, offsets, and state.
    /// Default: "./surgewave-data".
    /// </summary>
    public string DataDirectory { get; set; } = "./surgewave-data";

    /// <summary>
    /// Storage backend to use. Default: "File".
    /// Options: "File", "Memory", "ZeroCopyWal", "Arrow".
    /// </summary>
    public string Storage { get; set; } = "File";

    /// <summary>
    /// Storage configuration options.
    /// </summary>
    public StorageOptions StorageOptions { get; set; } = new();

    // ==================== Topics ====================

    /// <summary>
    /// Topic configuration options.
    /// </summary>
    public TopicOptions Topics { get; set; } = new();

    /// <summary>
    /// Whether to automatically create topics when they don't exist.
    /// Default: true.
    /// </summary>
    public bool AutoCreateTopics { get; set; } = true;

    // ==================== Retention ====================

    /// <summary>
    /// Log retention configuration.
    /// </summary>
    public RetentionOptions Retention { get; set; } = new();

    // ==================== Cluster ====================

    /// <summary>
    /// Cluster configuration (optional).
    /// </summary>
    public ClusterOptions? Cluster { get; set; }

    // ==================== Security ====================

    /// <summary>
    /// Security configuration (optional).
    /// </summary>
    public SecurityOptions? Security { get; set; }

    // ==================== Tiered Storage ====================

    /// <summary>
    /// Tiered storage configuration (optional).
    /// </summary>
    public TieredStorageOptions? TieredStorage { get; set; }

    /// <summary>
    /// Convert the Storage string to a well-known storage engine name.
    /// </summary>
    internal string GetStorageEngine() => Storage?.ToLowerInvariant() switch
    {
        "memory" => StorageEngines.Memory,
        "file" => StorageEngines.File,
        "zerocopywal" or "wal" => StorageEngines.ZeroCopyWal,
        "zerocopymemory" => StorageEngines.ZeroCopyMemory,
        _ when !string.IsNullOrEmpty(Storage) => Storage.ToLowerInvariant(),
        _ => StorageEngines.File
    };
}

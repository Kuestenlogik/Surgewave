using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Configuration for the Connect worker.
/// </summary>
public sealed class ConnectWorkerConfig : IValidatableConfig
{
    /// <summary>
    /// Bootstrap servers for connecting to Kafka/Surgewave.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>
    /// Consumer group ID for connector offset management.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string GroupId { get; set; } = "surgewave-connect";

    /// <summary>
    /// Topic for storing connector configurations.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string ConfigTopic { get; set; } = "surgewave-connect-configs";

    /// <summary>
    /// Topic for storing connector offsets.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string OffsetsTopic { get; set; } = "surgewave-connect-offsets";

    /// <summary>
    /// Topic for storing connector status.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string StatusTopic { get; set; } = "surgewave-connect-status";

    /// <summary>
    /// REST API port for managing connectors.
    /// </summary>
    [Range(1, 65535)]
    public int RestPort { get; set; } = 8083;

    /// <summary>
    /// Directories containing connector plugin assemblies.
    /// Each directory and its subdirectories will be scanned.
    /// Takes precedence over PluginsDirectory if both are set.
    /// Can also be set via Surgewave_PLUGIN_PATH environment variable (semicolon-separated on Windows, colon on Linux).
    /// </summary>
    public string[]? PluginsDirectories { get; set; }

    /// <summary>
    /// Directory containing connector plugin assemblies (legacy, single directory).
    /// Use PluginsDirectories for multiple directories.
    /// Each subdirectory represents a plugin.
    /// </summary>
    public string PluginsDirectory { get; set; } = "plugins";

    /// <summary>
    /// Whether to automatically scan for new plugins on startup.
    /// </summary>
    public bool EnablePluginDiscovery { get; set; } = true;

    /// <summary>
    /// Whether to run in distributed mode with worker coordination.
    /// When false, runs in standalone mode (single worker).
    /// </summary>
    public bool DistributedMode { get; set; } = false;

    /// <summary>
    /// Interval between worker heartbeats in milliseconds.
    /// </summary>
    [Range(100, int.MaxValue)]
    public int HeartbeatIntervalMs { get; set; } = 3000;

    /// <summary>
    /// Session timeout for worker liveness in milliseconds.
    /// If no heartbeat is received within this period, worker is considered dead.
    /// </summary>
    [Range(100, int.MaxValue)]
    public int SessionTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Rebalance delay in milliseconds before triggering task reassignment.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int RebalanceDelayMs { get; set; } = 1000;

    // === Worker Identity & Placement ===

    /// <summary>
    /// Role tags for this worker, used for pipeline placement decisions.
    /// Examples: "edge", "gpu", "high-memory", "region-eu", "secure-zone".
    /// </summary>
    public string[] Tags { get; set; } = [];

    /// <summary>
    /// Whether this worker allows on-demand plugin auto-installation
    /// when a pipeline requires a plugin that is not locally available.
    /// </summary>
    public bool AllowAutoInstall { get; set; } = false;

    // === Dead Letter Queue Settings ===

    /// <summary>
    /// Whether DLQ routing is enabled by default for connectors.
    /// </summary>
    public bool EnableDlq { get; set; } = true;

    /// <summary>
    /// Default DLQ topic suffix.
    /// </summary>
    public string DlqTopicSuffix { get; set; } = ".DLQ";

    /// <summary>
    /// Default maximum retry attempts before DLQ routing.
    /// </summary>
    [Range(0, 1000)]
    public int DlqMaxRetries { get; set; } = 3;

    /// <summary>
    /// Default retry backoff in milliseconds.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int DlqRetryBackoffMs { get; set; } = 1000;

    // === Exactly-Once Semantics Settings ===

    /// <summary>
    /// Whether to enable exactly-once semantics (EOS) for connectors.
    /// When enabled, source connectors use transactional producers and
    /// sink connectors use READ_COMMITTED consumers with atomic offset commits.
    /// Can be overridden per-connector with "exactly.once" config.
    /// </summary>
    public bool ExactlyOnceSupport { get; set; } = false;

    /// <summary>
    /// Prefix for transactional IDs used by EOS connectors.
    /// Each connector task will have a unique transactional ID:
    /// {TransactionIdPrefix}-{connectorName}-{taskId}
    /// </summary>
    [Required]
    [MinLength(1)]
    public string TransactionIdPrefix { get; set; } = "connect";

    /// <summary>
    /// Transaction timeout in milliseconds for EOS connectors.
    /// Transactions not completed within this time will be aborted.
    /// </summary>
    [Range(1_000, int.MaxValue)]
    public int TransactionTimeoutMs { get; set; } = 60000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        // Cross-property rules
        if (SessionTimeoutMs <= HeartbeatIntervalMs)
        {
            errors.Add($"{nameof(SessionTimeoutMs)}: must be greater than " +
                       $"{nameof(HeartbeatIntervalMs)} ({HeartbeatIntervalMs}ms).");
        }

        if (ExactlyOnceSupport && TransactionTimeoutMs < HeartbeatIntervalMs * 3)
        {
            errors.Add($"{nameof(TransactionTimeoutMs)}: should be at least 3x " +
                       $"{nameof(HeartbeatIntervalMs)} when {nameof(ExactlyOnceSupport)} is enabled " +
                       $"(have {TransactionTimeoutMs}ms, need >= {HeartbeatIntervalMs * 3}ms).");
        }

        if (PluginsDirectories != null && PluginsDirectories.Any(string.IsNullOrWhiteSpace))
        {
            errors.Add($"{nameof(PluginsDirectories)}: must not contain empty entries.");
        }

        return errors;
    }
}

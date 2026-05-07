using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Connect.Worker;

/// <summary>
/// Configuration for the standalone Connect worker.
/// </summary>
public sealed class StandaloneWorkerConfig : IValidatableConfig
{
    /// <summary>
    /// Bootstrap servers to connect to (comma-separated).
    /// </summary>
    [Required]
    [MinLength(1)]
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>
    /// Group ID for the Connect worker cluster.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string GroupId { get; set; } = "surgewave-connect";

    /// <summary>
    /// Topic for storing connector configurations.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string ConfigTopic { get; set; } = "_connect-configs";

    /// <summary>
    /// Topic for storing connector offsets.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string OffsetTopic { get; set; } = "_connect-offsets";

    /// <summary>
    /// Topic for storing connector status.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string StatusTopic { get; set; } = "_connect-status";

    /// <summary>
    /// REST API listen address.
    /// </summary>
    [Required]
    [Url]
    public string RestListenAddress { get; set; } = "http://localhost:8083";

    /// <summary>
    /// Plugin directories to scan for connectors (semicolon-separated).
    /// </summary>
    [Required]
    [MinLength(1)]
    public string PluginPath { get; set; } = "./plugins";

    /// <summary>
    /// Worker ID (defaults to hostname).
    /// </summary>
    [Required]
    [MinLength(1)]
    public string WorkerId { get; set; } = Environment.MachineName;

    /// <summary>
    /// Maximum retries for DLQ before routing to dead letter topic.
    /// </summary>
    [Range(0, 1000)]
    public int DlqMaxRetries { get; set; } = 3;

    /// <summary>
    /// Backoff between retries in milliseconds.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int DlqRetryBackoffMs { get; set; } = 1000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

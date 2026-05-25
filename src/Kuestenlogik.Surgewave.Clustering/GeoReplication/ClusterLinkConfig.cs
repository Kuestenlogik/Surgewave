using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// Configuration for a single cluster link used in geo-replication.
/// </summary>
public sealed class ClusterLinkConfig : IValidatableConfig
{
    /// <summary>
    /// Unique identifier for this cluster link.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string LinkId { get; set; } = "";

    /// <summary>
    /// Bootstrap servers of the remote cluster (comma-separated host:port pairs).
    /// </summary>
    [Required]
    [MinLength(1)]
    public string RemoteBootstrapServers { get; set; } = "";

    /// <summary>
    /// Identifier of the remote cluster (optional, for display/logging).
    /// </summary>
    public string? RemoteClusterId { get; set; }

    /// <summary>
    /// Regex pattern to filter topics for replication. Only matching topics are mirrored.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string TopicFilter { get; set; } = ".*";

    /// <summary>
    /// Topics to explicitly exclude from replication (blacklist).
    /// </summary>
    public string[] TopicExcludes { get; set; } = [];

    /// <summary>
    /// Interval in milliseconds between fetch requests to the remote cluster.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int FetchIntervalMs { get; set; } = 500;

    /// <summary>
    /// Maximum bytes to fetch per request from the remote cluster.
    /// </summary>
    [Range(1024, int.MaxValue)]
    public int FetchMaxBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Number of fetcher threads per link.
    /// </summary>
    [Range(1, 1024)]
    public int FetcherThreads { get; set; } = 4;

    /// <summary>
    /// Interval in milliseconds for metadata synchronization (topic discovery).
    /// </summary>
    [Range(100, int.MaxValue)]
    public int MetadataSyncIntervalMs { get; set; } = 30_000;

    /// <summary>
    /// Interval in milliseconds for consumer offset synchronization.
    /// </summary>
    [Range(100, int.MaxValue)]
    public int ConsumerOffsetSyncIntervalMs { get; set; } = 10_000;

    /// <summary>
    /// Whether to synchronize consumer group offsets from the remote cluster.
    /// </summary>
    public bool SyncConsumerOffsets { get; set; } = true;

    /// <summary>
    /// Whether to synchronize topic configurations from the remote cluster.
    /// </summary>
    public bool SyncTopicConfigs { get; set; } = true;

    /// <summary>
    /// Whether to synchronize ACLs from the remote cluster (Phase 2).
    /// </summary>
    public bool SyncAcls { get; set; } = false;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (TopicExcludes.Any(string.IsNullOrWhiteSpace))
            errors.Add($"{nameof(TopicExcludes)}: must not contain empty entries.");

        return errors;
    }
}

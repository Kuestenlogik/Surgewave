using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Configuration for broker-level Dead Letter Queue management.
/// Tracks per-message retry counts and routes messages to DLQ topics
/// after exceeding the maximum retry threshold.
/// </summary>
public sealed class DlqManagerConfig : IValidatableConfig
{
    /// <summary>
    /// Enable broker-level DLQ management.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Maximum retry attempts before routing to DLQ.
    /// Default: 3.
    /// </summary>
    [Range(0, 1_000)]
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Backoff delay in milliseconds between retries.
    /// Default: 1000 ms (1 second).
    /// </summary>
    [Range(0, long.MaxValue)]
    public long RetryBackoffMs { get; set; } = 1000;

    /// <summary>
    /// Suffix appended to the original topic name to form the DLQ topic name.
    /// Default: ".DLQ".
    /// </summary>
    [Required]
    [MinLength(1)]
    public string TopicSuffix { get; set; } = ".DLQ";

    /// <summary>
    /// Interval in milliseconds for cleaning up old retry tracking entries.
    /// Default: 60,000 ms (1 minute).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int CleanupIntervalMs { get; set; } = 60_000;

    /// <summary>
    /// Maximum age in milliseconds for retry tracking entries before cleanup.
    /// Default: 300,000 ms (5 minutes).
    /// </summary>
    [Range(1, long.MaxValue)]
    public long EntryMaxAgeMs { get; set; } = 300_000;

    /// <summary>
    /// Whether a missing DLQ topic is created automatically. Default: false.
    /// </summary>
    /// <remarks>
    /// Creating topics unasked is a problem rather than a convenience wherever topics are declared:
    /// a GitOps-managed cluster ends up with a topic nothing owns, on defaults nobody chose. With
    /// this off, a missing DLQ topic is reported and the record is not routed.
    /// </remarks>
    public bool AutoCreateTopics { get; set; }

    /// <summary>
    /// Partition count for an auto-created DLQ topic. Default: 1.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int DlqPartitionCount { get; set; } = 1;

    /// <summary>
    /// Retention for an auto-created DLQ topic in milliseconds. Default: 7 days.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long RetentionMs { get; set; } = 7 * 24 * 60 * 60 * 1000L;

    /// <summary>
    /// Fallback ceiling in bytes for a record read back for DLQ routing, used only when the DLQ
    /// topic declares no <c>max.message.bytes</c> of its own. Default: 1 MiB.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxRecordBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Get the DLQ topic name for a given original topic.
    /// </summary>
    public string GetDlqTopicName(string originalTopic) => $"{originalTopic}{TopicSuffix}";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

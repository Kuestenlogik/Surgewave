using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Core.Dlq;

/// <summary>
/// Configuration for Dead Letter Queue behavior.
/// </summary>
public sealed class DlqConfig : IValidatableConfig
{
    /// <summary>
    /// Whether DLQ routing is enabled. Default: true.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Maximum retry attempts before routing to DLQ. Default: 3.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Backoff between retries in milliseconds. Default: 1000.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int RetryBackoffMs { get; set; } = 1000;

    /// <summary>
    /// Suffix for DLQ topic names. Default: ".DLQ".
    /// </summary>
    [Required]
    [MinLength(1)]
    public string TopicSuffix { get; set; } = ".DLQ";

    /// <summary>
    /// Whether to include the full stack trace in DLQ metadata. Default: true.
    /// </summary>
    public bool IncludeStackTrace { get; set; } = true;

    /// <summary>
    /// Number of partitions to create for auto-created DLQ topics. Default: 1.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int DlqPartitionCount { get; set; } = 1;

    /// <summary>
    /// Retention period for DLQ topics in milliseconds. Default: 7 days.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long RetentionMs { get; set; } = 604800000;

    /// <summary>
    /// Get the DLQ topic name for a given original topic.
    /// </summary>
    public string GetDlqTopicName(string originalTopic) => $"{originalTopic}{TopicSuffix}";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

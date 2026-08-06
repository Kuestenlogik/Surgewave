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
    /// Prefix for DLQ topic names. Default: "dlq.".
    /// </summary>
    /// <remarks>
    /// A prefix rather than a suffix because it makes the DLQ topics a NAMESPACE: one ACL and one
    /// quota can cover "dlq.*", which a suffix cannot express. <see cref="TopicSuffix"/> still wins
    /// when set, so an existing deployment keeps writing where it always did.
    /// </remarks>
    [Required]
    [MinLength(1)]
    public string TopicPrefix { get; set; } = "dlq.";

    /// <summary>
    /// Legacy suffix for DLQ topic names. Unset by default; when set it takes precedence over
    /// <see cref="TopicPrefix"/> so upgrading does not silently relocate a deployment's DLQ data.
    /// </summary>
    public string? TopicSuffix { get; set; }

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
    /// Whether the failed record's VALUE and headers are copied into the DLQ record.
    /// Default: false — the DLQ record carries only what identifies the failure.
    /// </summary>
    /// <remarks>
    /// Off by default for two reasons that have nothing to do with disk. A copy doubles the
    /// retention obligation for data the operator may not be allowed to keep twice, under a topic
    /// with different ACLs and a different retention; and the payload is exactly what tends to
    /// carry personal data. What a DLQ is FOR — which record failed, where, why, how often — is all
    /// metadata. An operator who needs the payload to reproduce a failure can say so.
    /// </remarks>
    public bool CopyRecordValue { get; set; }

    /// <summary>
    /// Whether a missing DLQ topic is created automatically. Default: false.
    /// </summary>
    /// <remarks>
    /// Creating topics unasked is a problem rather than a convenience wherever topics are
    /// declared — a GitOps-managed cluster gets a topic nothing owns, with defaults nobody chose.
    /// </remarks>
    public bool AutoCreateTopics { get; set; }

    /// <summary>
    /// Fallback ceiling in bytes for a record read back for DLQ routing, used only when the DLQ
    /// topic declares no <c>max.message.bytes</c> of its own. Default: 1 MiB.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxRecordBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Get the DLQ topic name for a given original topic.
    /// </summary>
    public string GetDlqTopicName(string originalTopic) =>
        string.IsNullOrEmpty(TopicSuffix)
            ? $"{TopicPrefix}{originalTopic}"
            : $"{originalTopic}{TopicSuffix}";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

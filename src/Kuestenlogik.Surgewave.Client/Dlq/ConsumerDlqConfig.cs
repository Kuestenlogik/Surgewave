using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Client.Dlq;

/// <summary>
/// DLQ configuration for SurgewaveConsumer.
/// </summary>
public sealed class ConsumerDlqConfig : IValidatableConfig
{
    /// <summary>
    /// Whether to enable DLQ routing for handler failures. Default: false.
    /// </summary>
    public bool EnableDlq { get; set; } = false;

    /// <summary>
    /// Maximum retries before DLQ routing. Default: 3.
    /// </summary>
    [Range(0, 1_000)]
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Retry backoff in milliseconds. Default: 1000.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int RetryBackoffMs { get; set; } = 1000;

    /// <summary>
    /// DLQ topic suffix. Default: ".DLQ".
    /// </summary>
    [Required]
    [MinLength(1)]
    public string TopicSuffix { get; set; } = ".DLQ";

    /// <summary>
    /// Whether to include stack traces in DLQ records. Default: true.
    /// </summary>
    public bool IncludeStackTrace { get; set; } = true;

    /// <summary>
    /// Get the DLQ topic name for a given original topic.
    /// </summary>
    public string GetDlqTopicName(string originalTopic) => $"{originalTopic}{TopicSuffix}";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

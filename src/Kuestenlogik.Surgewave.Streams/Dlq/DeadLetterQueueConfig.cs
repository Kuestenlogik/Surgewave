using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Streams.Dlq;

/// <summary>
/// Configuration for dead letter queue behavior.
/// </summary>
public sealed class DeadLetterQueueConfig : IValidatableConfig
{
    public bool Enabled { get; init; }

    [Required]
    [MinLength(1)]
    public string TopicSuffix { get; init; } = ".DLQ";

    [Range(0, 1_000)]
    public int MaxRetries { get; init; }

    public bool IncludeStackTrace { get; init; } = true;
    public bool IncludeHeaders { get; init; } = true;

    public string GetDlqTopicName(string sourceTopic) => $"{sourceTopic}{TopicSuffix}";

    public static DeadLetterQueueConfig Disabled => new() { Enabled = false };

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker.Queue;

/// <summary>
/// Configuration for QueueView — RabbitMQ/SQS-style queue semantics on top of the Surgewave log.
/// Messages remain in the normal log (replay stays possible); QueueView tracks in-flight state only.
/// </summary>
public sealed class QueueViewConfig : IValidatableConfig
{
    /// <summary>Configuration section name: <c>Surgewave:QueueView</c>.</summary>
    public const string SectionName = "Surgewave:QueueView";

    /// <summary>
    /// Enable QueueView semantics for topics that are explicitly enrolled.
    /// Default: false.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// How long a delivered message is hidden from other consumers.
    /// If not acknowledged within this window the message becomes visible again.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum number of delivery attempts before a message is routed to the DLQ topic.
    /// Default: 5.
    /// </summary>
    [Range(1, 1000)]
    public int MaxDeliveryCount { get; init; } = 5;

    /// <summary>
    /// Suffix appended to the source topic name to derive the Dead-Letter-Queue topic name.
    /// Default: ".dlq".
    /// </summary>
    public string? DlqTopicSuffix { get; init; } = ".dlq";

    /// <summary>
    /// How often the background worker checks for expired visibility timeouts.
    /// Default: 10 seconds.
    /// </summary>
    public TimeSpan CleanupInterval { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Maximum number of in-flight (delivered-but-not-yet-acknowledged) messages per consumer.
    /// Default: 1000.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxInFlightPerConsumer { get; init; } = 1000;

    /// <summary>
    /// Returns the DLQ topic name for a given source topic.
    /// </summary>
    public string GetDlqTopicName(string sourceTopic) =>
        $"{sourceTopic}{DlqTopicSuffix ?? ".dlq"}";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (VisibilityTimeout <= TimeSpan.Zero)
            errors.Add($"{nameof(VisibilityTimeout)}: must be positive.");

        if (CleanupInterval <= TimeSpan.Zero)
            errors.Add($"{nameof(CleanupInterval)}: must be positive.");

        return errors;
    }
}

namespace Kuestenlogik.Surgewave.Core.Dlq;

/// <summary>
/// A record routed to the Dead Letter Queue with full error context.
/// </summary>
public sealed record DlqRecord
{
    /// <summary>
    /// Original topic the message came from.
    /// </summary>
    public required string OriginalTopic { get; init; }

    /// <summary>
    /// Original partition.
    /// </summary>
    public required int OriginalPartition { get; init; }

    /// <summary>
    /// Original offset.
    /// </summary>
    public required long OriginalOffset { get; init; }

    /// <summary>
    /// Original message key (may be null).
    /// </summary>
    public byte[]? OriginalKey { get; init; }

    /// <summary>
    /// Original message value.
    /// </summary>
    public required byte[] OriginalValue { get; init; }

    /// <summary>
    /// Original message timestamp.
    /// </summary>
    public DateTimeOffset OriginalTimestamp { get; init; }

    /// <summary>
    /// Original message headers.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]>? OriginalHeaders { get; init; }

    /// <summary>
    /// Exception type that caused the failure.
    /// </summary>
    public required string ExceptionType { get; init; }

    /// <summary>
    /// Exception message.
    /// </summary>
    public required string ExceptionMessage { get; init; }

    /// <summary>
    /// Full stack trace (if configured to include).
    /// </summary>
    public string? StackTrace { get; init; }

    /// <summary>
    /// Name of the connector or consumer group that failed.
    /// </summary>
    public required string SourceName { get; init; }

    /// <summary>
    /// Type of source: "connect-sink", "connect-source", "consumer".
    /// </summary>
    public required string SourceType { get; init; }

    /// <summary>
    /// Task ID (for Connect) or consumer instance ID.
    /// </summary>
    public string? TaskId { get; init; }

    /// <summary>
    /// Number of retry attempts before routing to DLQ.
    /// </summary>
    public int AttemptCount { get; init; }

    /// <summary>
    /// Timestamp when the record was routed to DLQ.
    /// </summary>
    public DateTimeOffset FailedAt { get; init; }

    /// <summary>
    /// Additional context metadata.
    /// </summary>
    public IReadOnlyDictionary<string, string>? AdditionalContext { get; init; }
}

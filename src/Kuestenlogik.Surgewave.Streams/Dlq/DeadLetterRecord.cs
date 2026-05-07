namespace Kuestenlogik.Surgewave.Streams.Dlq;

/// <summary>
/// Represents a record that failed processing and was sent to a dead letter queue.
/// </summary>
public sealed record DeadLetterRecord
{
    public required string OriginalTopic { get; init; }
    public required int OriginalPartition { get; init; }
    public required long OriginalOffset { get; init; }
    public required byte[] Key { get; init; }
    public required byte[] Value { get; init; }
    public required long Timestamp { get; init; }
    public required string ApplicationId { get; init; }
    public required string ExceptionType { get; init; }
    public required string ExceptionMessage { get; init; }
    public string? StackTrace { get; init; }
    public int RetryCount { get; init; }
    public DateTimeOffset FailedAt { get; init; } = DateTimeOffset.UtcNow;
}

namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// Interface for tracking processed message IDs to enable deduplication.
/// Implementations can use in-memory, persistent, or distributed storage.
/// </summary>
public interface IDeduplicationStore : IAsyncDisposable
{
    /// <summary>
    /// Checks if a message has already been processed.
    /// </summary>
    /// <param name="messageId">The unique message identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the message was already processed, false otherwise.</returns>
    Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as processed.
    /// </summary>
    /// <param name="messageId">The unique message identifier.</param>
    /// <param name="metadata">Optional metadata about when/where the message was processed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkProcessedAsync(string messageId, ProcessedMetadata? metadata = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks multiple messages as processed in a batch.
    /// </summary>
    /// <param name="messageIds">The message identifiers to mark.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkProcessedBatchAsync(IEnumerable<string> messageIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes old entries from the store based on age.
    /// </summary>
    /// <param name="maxAge">Maximum age of entries to keep.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of entries removed.</returns>
    Task<int> CleanupAsync(TimeSpan maxAge, CancellationToken cancellationToken = default);
}

/// <summary>
/// Metadata about a processed message.
/// </summary>
public sealed record ProcessedMetadata
{
    /// <summary>
    /// When the message was processed.
    /// </summary>
    public DateTimeOffset ProcessedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// The source topic.
    /// </summary>
    public string? Topic { get; init; }

    /// <summary>
    /// The source partition.
    /// </summary>
    public int? Partition { get; init; }

    /// <summary>
    /// The source offset.
    /// </summary>
    public long? Offset { get; init; }
}

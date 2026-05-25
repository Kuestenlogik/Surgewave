namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// Interface for idempotent write operations to external systems.
/// Implementing this interface allows sink connectors to achieve end-to-end exactly-once semantics
/// by ensuring that duplicate writes are handled gracefully.
/// </summary>
/// <typeparam name="TKey">The type of the record key used for deduplication.</typeparam>
/// <typeparam name="TValue">The type of the record value to write.</typeparam>
public interface IIdempotentWriter<TKey, TValue> : IAsyncDisposable
    where TKey : notnull
{
    /// <summary>
    /// Writes a record idempotently. If a record with the same key exists and is identical,
    /// this operation should be a no-op. If the record differs, it should be updated (upsert).
    /// </summary>
    /// <param name="key">The unique key for deduplication.</param>
    /// <param name="value">The value to write.</param>
    /// <param name="metadata">Additional metadata about the record.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the write operation.</returns>
    Task<WriteResult> WriteAsync(TKey key, TValue value, WriteMetadata metadata, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes multiple records idempotently in a batch.
    /// </summary>
    /// <param name="records">The records to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Results for each record in the batch.</returns>
    Task<IReadOnlyList<WriteResult>> WriteBatchAsync(IReadOnlyList<WriteRecord<TKey, TValue>> records, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if a record with the given key already exists.
    /// </summary>
    /// <param name="key">The key to check.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the key exists, false otherwise.</returns>
    Task<bool> ExistsAsync(TKey key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a record with the given key if it exists.
    /// </summary>
    /// <param name="key">The key to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the record was deleted, false if it didn't exist.</returns>
    Task<bool> DeleteAsync(TKey key, CancellationToken cancellationToken = default);
}

/// <summary>
/// A record to write idempotently.
/// </summary>
public sealed record WriteRecord<TKey, TValue>(TKey Key, TValue Value, WriteMetadata Metadata)
    where TKey : notnull;

/// <summary>
/// Metadata about a write operation.
/// </summary>
public sealed record WriteMetadata
{
    /// <summary>
    /// The source topic of the record.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The source partition.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// The source offset.
    /// </summary>
    public required long Offset { get; init; }

    /// <summary>
    /// The record timestamp.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Creates metadata from a SinkRecord.
    /// </summary>
    public static WriteMetadata FromSinkRecord(SinkRecord record) => new()
    {
        Topic = record.Topic,
        Partition = record.Partition,
        Offset = record.Offset,
        Timestamp = record.Timestamp
    };
}

/// <summary>
/// Result of a write operation.
/// </summary>
public sealed record WriteResult
{
    /// <summary>
    /// Whether the write was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// The type of operation that was performed.
    /// </summary>
    public required WriteOperation Operation { get; init; }

    /// <summary>
    /// Error message if the write failed.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// Creates a successful write result.
    /// </summary>
    public static WriteResult Inserted() => new() { Success = true, Operation = WriteOperation.Inserted };

    /// <summary>
    /// Creates a successful update result.
    /// </summary>
    public static WriteResult Updated() => new() { Success = true, Operation = WriteOperation.Updated };

    /// <summary>
    /// Creates a skipped result (duplicate detected).
    /// </summary>
    public static WriteResult Skipped() => new() { Success = true, Operation = WriteOperation.Skipped };

    /// <summary>
    /// Creates a failed result.
    /// </summary>
    public static WriteResult Failed(string error) => new() { Success = false, Operation = WriteOperation.Failed, Error = error };
}

/// <summary>
/// The type of write operation that was performed.
/// </summary>
public enum WriteOperation
{
    /// <summary>
    /// A new record was inserted.
    /// </summary>
    Inserted,

    /// <summary>
    /// An existing record was updated.
    /// </summary>
    Updated,

    /// <summary>
    /// The write was skipped because an identical record already exists.
    /// </summary>
    Skipped,

    /// <summary>
    /// The write failed.
    /// </summary>
    Failed
}

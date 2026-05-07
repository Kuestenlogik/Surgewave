namespace Kuestenlogik.Surgewave.Storage;

/// <summary>
/// Zero-copy storage engine abstraction for Surgewave.
///
/// Key design principles:
/// - Returns ISurgewaveBuffer instead of byte[] for zero-copy reads
/// - Accepts ReadOnlySpan for writes (caller owns data)
/// - Explicit lifetime management via IDisposable buffers
///
/// Implementations:
/// - MemoryStorageEngine: In-memory with ArrayPool backing
/// - WalStorageEngine: File-based with memory-mapped reads
/// - ArrowStorageEngine: Arrow columnar storage (separate package)
/// </summary>
public interface ISurgewaveStorageEngine : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Base offset of this storage segment.
    /// </summary>
    long BaseOffset { get; }

    /// <summary>
    /// Current write offset (next offset to be written).
    /// </summary>
    long CurrentOffset { get; }

    /// <summary>
    /// Total size of stored data in bytes.
    /// </summary>
    long Size { get; }

    /// <summary>
    /// Whether this storage segment has reached capacity.
    /// </summary>
    bool IsFull { get; }

    /// <summary>
    /// When this segment was created.
    /// </summary>
    DateTime CreatedAt { get; }

    /// <summary>
    /// Maximum timestamp of any record in this segment.
    /// </summary>
    long MaxTimestamp { get; }

    /// <summary>
    /// Get the offset of the first record, or null if empty.
    /// </summary>
    long? FirstOffset { get; }

    /// <summary>
    /// Append a Kafka RecordBatch to storage.
    /// The data is copied; caller retains ownership of the span.
    /// </summary>
    /// <param name="recordBatch">Raw Kafka RecordBatch bytes</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Base offset and record count of the appended batch</returns>
    ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ReadOnlySpan<byte> recordBatch,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Append a Kafka RecordBatch from a buffer.
    /// The implementation may take ownership of the buffer to avoid copying.
    /// </summary>
    ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ISurgewaveBuffer recordBatch,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Read record batches starting from an offset.
    /// Returns a buffer that MUST be disposed after use.
    /// </summary>
    /// <param name="startOffset">Starting offset</param>
    /// <param name="maxBytes">Maximum bytes to read</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>
    /// A lease containing the data buffer and batch boundaries.
    /// The lease must be disposed when done reading.
    /// </returns>
    ValueTask<IStorageReadLease> ReadAsync(
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Force flush all pending writes to durable storage.
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Find the offset of the first batch with timestamp >= targetTimestamp.
    /// </summary>
    long? FindOffsetByTimestamp(long targetTimestamp);

    /// <summary>
    /// Delete all storage files/resources.
    /// Must be called after disposal.
    /// </summary>
    void DeleteStorage();
}

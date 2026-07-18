using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Abstraction for a partition log. Implemented by PartitionLog (persistent/memory segment-based)
/// and EphemeralPartitionLog (ring-buffer, no persistence).
/// </summary>
public interface IPartitionLog : IDisposable
{
    /// <summary>Gets the topic and partition this log belongs to.</summary>
    TopicPartition TopicPartition { get; }

    /// <summary>Gets the next offset that will be assigned to an appended record.</summary>
    long NextOffset { get; }

    /// <summary>Gets the high watermark (last committed offset visible to consumers).</summary>
    long HighWatermark { get; }

    /// <summary>Gets the earliest available offset (may advance due to retention policies).</summary>
    long LogStartOffset { get; }

    /// <summary>Gets the total size of the log in bytes across all segments.</summary>
    long TotalSize { get; }

    /// <summary>Appends a record batch to the log.</summary>
    /// <param name="recordBatch">The raw record batch bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The base offset assigned to the batch.</returns>
    ValueTask<long> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default);

    /// <summary>Appends a slice of a record batch buffer to the log (zero-copy for pooled buffers).</summary>
    /// <param name="buffer">The buffer containing the record batch.</param>
    /// <param name="offset">The start offset within the buffer.</param>
    /// <param name="length">The number of bytes to append.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The base offset assigned to the batch.</returns>
    ValueTask<long> AppendBatchAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default);

    /// <summary>Appends a slice of a record batch buffer with explicit CRC handling (#85).</summary>
    /// <param name="buffer">The buffer containing the record batch.</param>
    /// <param name="offset">The start offset within the buffer.</param>
    /// <param name="length">The number of bytes to append.</param>
    /// <param name="crcMode">
    /// <see cref="BatchCrcMode.Validate"/> checks the producer's CRC and rejects corrupt batches;
    /// <see cref="BatchCrcMode.Trusted"/> skips the pass for serializer-fresh bytes;
    /// <see cref="BatchCrcMode.Recompute"/> keeps the legacy overwrite.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The base offset assigned to the batch.</returns>
    ValueTask<long> AppendBatchAsync(byte[] buffer, int offset, int length, BatchCrcMode crcMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends a record batch at a specific target offset (for offset-preserving geo-replication).
    /// The target offset must be greater than or equal to the current <see cref="NextOffset"/>.
    /// </summary>
    /// <param name="recordBatch">The raw record batch bytes.</param>
    /// <param name="targetOffset">The target offset to assign.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The base offset assigned to the batch.</returns>
    ValueTask<long> AppendBatchAtOffsetAsync(byte[] recordBatch, long targetOffset, CancellationToken cancellationToken = default);

    /// <summary>
    /// Offset-preserving append of a SLICE of a buffer (one batch of a split replication fetch
    /// section) with explicit CRC handling (#85). <see cref="BatchCrcMode.Validate"/> checks the
    /// batch's own CRC and rejects corruption; <see cref="BatchCrcMode.Recompute"/> keeps the legacy
    /// overwrite. <paramref name="targetOffset"/> must be &gt;= <see cref="NextOffset"/> — a larger
    /// value is a valid sparse gap.
    /// </summary>
    ValueTask<long> AppendBatchAtOffsetAsync(
        byte[] buffer, int offset, int length, long targetOffset, BatchCrcMode crcMode, CancellationToken cancellationToken = default);

    /// <summary>Reads record batches starting from the specified offset.</summary>
    /// <param name="startOffset">The offset to start reading from.</param>
    /// <param name="maxBytes">Maximum number of bytes to read (default: 1MB).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A list of raw record batch byte arrays.</returns>
    ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes = 1048576, CancellationToken cancellationToken = default);

    /// <summary>Reads record batches as a single contiguous memory block for zero-copy fetch.</summary>
    /// <param name="startOffset">The offset to start reading from.</param>
    /// <param name="maxBytes">Maximum number of bytes to read (default: 1MB).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The contiguous data and batch boundary offsets within it.</returns>
    ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes = 1048576, CancellationToken cancellationToken = default);

    /// <summary>Waits for data to become available at the specified offset.</summary>
    /// <param name="offset">The offset to wait for.</param>
    /// <param name="timeout">Maximum time to wait.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if data is available; false if the timeout elapsed.</returns>
    Task<bool> WaitForDataAsync(long offset, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>Finds the offset of the first record with a timestamp at or after the target.</summary>
    /// <param name="targetTimestamp">The target timestamp in Unix milliseconds.</param>
    /// <returns>The matching offset, or null if no matching record exists.</returns>
    long? FindOffsetByTimestamp(long targetTimestamp);
}

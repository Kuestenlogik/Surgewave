using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Core.Pipeline;

/// <summary>
/// Request to write a Kafka RecordBatch to a partition.
/// Fully pooled for zero-allocation in hot path.
/// Supports slices to avoid copying ArrayPool buffers.
/// </summary>
public sealed class WriteRequest
{
    private static readonly ConcurrentBag<WriteRequest> s_pool = new();
    private static int s_poolSize;
    private const int MaxPoolSize = 1024;

    public TopicPartition TopicPartition { get; private set; }
    public byte[] RecordBatch { get; private set; } = null!;
    public int RecordBatchOffset { get; private set; }
    public int RecordBatchLength { get; private set; }
    public PooledCompletionSource<long> CompletionSource { get; private set; } = null!;
    public CancellationToken CancellationToken { get; private set; }

    /// <summary>How the append should treat the batch's CRC field (#85).</summary>
    public BatchCrcMode CrcMode { get; private set; }

    /// <summary>
    /// Gets a Span view of the record batch slice.
    /// </summary>
    public Span<byte> RecordBatchSpan => RecordBatch.AsSpan(RecordBatchOffset, RecordBatchLength);

    private WriteRequest() { }

    /// <summary>
    /// Create a new write request from pool with a pooled completion source.
    /// </summary>
    public static WriteRequest Create(TopicPartition topicPartition, byte[] recordBatch, CancellationToken cancellationToken)
    {
        return Create(topicPartition, recordBatch, 0, recordBatch.Length, BatchCrcMode.Recompute, cancellationToken);
    }

    /// <summary>
    /// Create a new write request for a slice of a buffer. Zero-copy for ArrayPool buffers.
    /// </summary>
    public static WriteRequest Create(TopicPartition topicPartition, byte[] buffer, int offset, int length, CancellationToken cancellationToken)
    {
        return Create(topicPartition, buffer, offset, length, BatchCrcMode.Recompute, cancellationToken);
    }

    /// <summary>
    /// Create a new write request for a slice of a buffer with explicit CRC handling (#85).
    /// </summary>
    public static WriteRequest Create(TopicPartition topicPartition, byte[] buffer, int offset, int length, BatchCrcMode crcMode, CancellationToken cancellationToken)
    {
        if (!s_pool.TryTake(out var request))
        {
            request = new WriteRequest();
        }
        else
        {
            Interlocked.Decrement(ref s_poolSize);
        }

        request.TopicPartition = topicPartition;
        request.RecordBatch = buffer;
        request.RecordBatchOffset = offset;
        request.RecordBatchLength = length;
        request.CrcMode = crcMode;
        request.CompletionSource = PooledCompletionSource<long>.Rent();
        request.CancellationToken = cancellationToken;
        return request;
    }

    /// <summary>
    /// Return the request and completion source to their pools.
    /// </summary>
    public void ReturnToPool()
    {
        CompletionSource.Return();

        // Clear references to allow GC
        RecordBatch = null!;
        RecordBatchOffset = 0;
        RecordBatchLength = 0;
        TopicPartition = default;
        CancellationToken = default;
        // Must reset: a leaked Trusted would skip CRC handling on someone else's append.
        CrcMode = BatchCrcMode.Recompute;

        // Return to pool if not full
        if (Interlocked.Increment(ref s_poolSize) <= MaxPoolSize)
        {
            s_pool.Add(this);
        }
        else
        {
            Interlocked.Decrement(ref s_poolSize);
        }
    }
}

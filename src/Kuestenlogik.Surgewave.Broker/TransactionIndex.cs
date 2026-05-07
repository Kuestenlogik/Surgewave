using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Tracks transaction state per partition for proper READ_COMMITTED isolation.
///
/// Key concepts:
/// - In-flight transactions: Started but not yet committed/aborted
/// - LSO (Last Stable Offset): Highest offset where all prior transactions are complete
/// - Aborted transactions: Track producer IDs that aborted so we can skip their messages
///
/// For READ_COMMITTED:
/// - Consumer only sees messages up to LSO
/// - Messages from aborted transactions are skipped
/// </summary>
public sealed class TransactionIndex
{
    /// <summary>
    /// Tracks in-flight transactions per partition.
    /// Key: TopicPartition
    /// Value: Dictionary of ProducerId -> first transactional batch offset
    /// </summary>
    private readonly ConcurrentDictionary<TopicPartition, ConcurrentDictionary<long, long>>
        _inFlightTransactions = new();

    /// <summary>
    /// Tracks aborted transactions per partition.
    /// Key: TopicPartition
    /// Value: Set of AbortedTransaction (ProducerId + offset range)
    /// </summary>
    private readonly ConcurrentDictionary<TopicPartition, ConcurrentBag<AbortedTransaction>>
        _abortedTransactions = new();

    /// <summary>
    /// Last Stable Offset per partition - the highest offset where all prior transactions are complete
    /// </summary>
    private readonly ConcurrentDictionary<TopicPartition, long> _lastStableOffsets = new();

    /// <summary>
    /// Records that a transactional batch was written to a partition.
    /// Called when a Produce with transactional flag is processed.
    /// </summary>
    public void RecordTransactionalBatch(TopicPartition partition, long producerId, long baseOffset)
    {
        var partitionTxns = _inFlightTransactions.GetOrAdd(partition, _ => new ConcurrentDictionary<long, long>());

        // Only record the first offset for this producer's transaction
        partitionTxns.TryAdd(producerId, baseOffset);
    }

    /// <summary>
    /// Called when a transaction is committed.
    /// Advances LSO past all offsets from this producer.
    /// </summary>
    public void CommitTransaction(long producerId, IEnumerable<TopicPartition> partitions, long commitOffset)
    {
        foreach (var partition in partitions)
        {
            if (_inFlightTransactions.TryGetValue(partition, out var partitionTxns))
            {
                partitionTxns.TryRemove(producerId, out _);
            }

            // Recalculate LSO for this partition
            RecalculateLso(partition, commitOffset);
        }
    }

    /// <summary>
    /// Called when a transaction is aborted.
    /// Records the abort so READ_COMMITTED consumers can skip these messages.
    /// </summary>
    public void AbortTransaction(long producerId, IEnumerable<TopicPartition> partitions, long abortOffset)
    {
        foreach (var partition in partitions)
        {
            long firstOffset = 0;

            if (_inFlightTransactions.TryGetValue(partition, out var partitionTxns))
            {
                partitionTxns.TryRemove(producerId, out firstOffset);
            }

            // Record the aborted transaction range
            var abortedTxns = _abortedTransactions.GetOrAdd(partition, _ => new ConcurrentBag<AbortedTransaction>());
            abortedTxns.Add(new AbortedTransaction
            {
                ProducerId = producerId,
                FirstOffset = firstOffset
            });

            // Recalculate LSO for this partition
            RecalculateLso(partition, abortOffset);
        }
    }

    /// <summary>
    /// Get the Last Stable Offset for a partition.
    /// For READ_COMMITTED, only messages up to LSO should be returned.
    /// </summary>
    public long GetLastStableOffset(TopicPartition partition, long highWatermark)
    {
        // If no in-flight transactions, LSO = HW
        if (!_inFlightTransactions.TryGetValue(partition, out var partitionTxns) || partitionTxns.IsEmpty)
        {
            return highWatermark;
        }

        // LSO is the minimum of all in-flight transaction first offsets
        var minInFlightOffset = partitionTxns.Values.Min();

        // LSO is the offset just before the first in-flight transaction
        return Math.Min(minInFlightOffset, highWatermark);
    }

    /// <summary>
    /// Get list of aborted transactions that overlap with the offset range.
    /// Used to tell consumers which messages to skip.
    /// </summary>
    public List<AbortedTransaction> GetAbortedTransactions(TopicPartition partition, long startOffset, long endOffset)
    {
        if (!_abortedTransactions.TryGetValue(partition, out var abortedTxns))
        {
            return [];
        }

        // Return aborted transactions that might overlap with the requested range
        return abortedTxns
            .Where(at => at.FirstOffset <= endOffset)
            .ToList();
    }

    /// <summary>
    /// Check if a batch from a specific producer should be filtered out (was aborted).
    /// </summary>
    public bool IsBatchAborted(TopicPartition partition, long producerId, long batchBaseOffset)
    {
        if (!_abortedTransactions.TryGetValue(partition, out var abortedTxns))
        {
            return false;
        }

        // Check if this producer has an aborted transaction that includes this offset
        return abortedTxns.Any(at =>
            at.ProducerId == producerId &&
            batchBaseOffset >= at.FirstOffset);
    }

    /// <summary>
    /// Filter batches for READ_COMMITTED isolation.
    /// - Only return batches up to LSO
    /// - Filter out control batches
    /// - Filter out aborted transaction batches
    /// </summary>
    public List<byte[]> FilterForReadCommitted(
        TopicPartition partition,
        List<byte[]> batches,
        long highWatermark)
    {
        var lso = GetLastStableOffset(partition, highWatermark);
        var result = new List<byte[]>();

        foreach (var batch in batches)
        {
            // Skip control batches (transaction markers) - they're internal
            if (CompressionCodec.IsControlBatch(batch))
                continue;

            // Get batch base offset
            var batchOffset = GetBatchBaseOffset(batch);

            // Skip batches beyond LSO (uncommitted transactions)
            if (batchOffset >= lso)
                continue;

            // Check if this is a transactional batch that was aborted
            if (CompressionCodec.IsTransactional(batch))
            {
                var (producerId, _, _, _) = CompressionCodec.GetIdempotenceInfo(batch);

                if (IsBatchAborted(partition, producerId, batchOffset))
                    continue; // Skip aborted transaction batches
            }

            result.Add(batch);
        }

        return result;
    }

    /// <summary>
    /// Filter batches for READ_UNCOMMITTED isolation.
    /// - Only filter out control batches
    /// </summary>
    public List<byte[]> FilterForReadUncommitted(List<byte[]> batches)
    {
        var result = new List<byte[]>();

        foreach (var batch in batches)
        {
            // Skip control batches (transaction markers) - they're internal
            if (CompressionCodec.IsControlBatch(batch))
                continue;

            result.Add(batch);
        }

        return result;
    }

    private void RecalculateLso(TopicPartition partition, long currentOffset)
    {
        // LSO advances when a transaction completes
        if (!_inFlightTransactions.TryGetValue(partition, out var partitionTxns) || partitionTxns.IsEmpty)
        {
            _lastStableOffsets[partition] = currentOffset;
        }
        else
        {
            var minInFlightOffset = partitionTxns.Values.Min();
            _lastStableOffsets[partition] = minInFlightOffset;
        }
    }

    private static long GetBatchBaseOffset(byte[] batch)
    {
        if (batch.Length < 8)
            return 0;

        return System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(0, 8));
    }
}

/// <summary>
/// Represents an aborted transaction for a specific producer.
/// </summary>
public record struct AbortedTransaction
{
    public long ProducerId { get; init; }
    public long FirstOffset { get; init; }
}

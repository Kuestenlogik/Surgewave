using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Broker.Tests.Fakes;

/// <summary>
/// A segment decorator whose <see cref="ReadContiguousAsync"/> serves the batches from a borrowed
/// buffer instead of a fresh array — the shape the pooled and memory-mapped engines use (#78).
///
/// <para>Releasing the lease scribbles over that buffer, which is what makes the borrowing
/// observable: a consumer that copied sees intact bytes afterwards, one that borrowed sees the
/// scribble. Real pools are less obliging — they hand the buffer to someone else — so this is the
/// deterministic stand-in for "read after release returns foreign data".</para>
/// </summary>
internal sealed class LeaseTrackingLogSegment(ILogSegment inner, LeaseTracker tracker) : ILogSegment
{
    /// <summary>The byte a released lease leaves behind in its buffer.</summary>
    internal const byte ReleasedFill = 0xCC;

    public long BaseOffset => inner.BaseOffset;
    public long CurrentOffset => inner.CurrentOffset;
    public long Size => inner.Size;
    public bool IsFull => inner.IsFull;
    public DateTime CreatedAt => inner.CreatedAt;
    public long MaxTimestamp => inner.MaxTimestamp;

    public long? GetFirstMessageOffset() => inner.GetFirstMessageOffset();

    public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default)
        => inner.AppendBatchAsync(recordBatch, cancellationToken);

    public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken cancellationToken = default)
        => inner.AppendBatchAsync(recordBatch, cancellationToken);

    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => inner.FlushAsync(cancellationToken);

    public ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
        => inner.ReadBatchesAsync(startOffset, maxBytes, cancellationToken);

    public ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
        => inner.ReadBatchesContiguousAsync(startOffset, maxBytes, cancellationToken);

    public async ValueTask<ContiguousBatchRead> ReadContiguousAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
    {
        var (data, batchOffsets) = await inner.ReadBatchesContiguousAsync(startOffset, maxBytes, cancellationToken);

        if (data.Length == 0)
            return ContiguousBatchRead.Empty;

        var borrowed = new byte[data.Length];
        data.Span.CopyTo(borrowed);

        tracker.Acquired();
        return new ContiguousBatchRead(borrowed, batchOffsets, new TrackedLease(borrowed, tracker));
    }

    public long? GetFilePositionForOffset(long startOffset) => inner.GetFilePositionForOffset(startOffset);

    public long? FindOffsetByTimestamp(long targetTimestamp) => inner.FindOffsetByTimestamp(targetTimestamp);

    public void DeleteFiles() => inner.DeleteFiles();

    public void Dispose() => inner.Dispose();

    private sealed class TrackedLease(byte[] buffer, LeaseTracker tracker) : IDisposable
    {
        private bool _released;

        public void Dispose()
        {
            if (_released)
                return;

            _released = true;
            Array.Fill(buffer, ReleasedFill);
            tracker.Released();
        }
    }
}

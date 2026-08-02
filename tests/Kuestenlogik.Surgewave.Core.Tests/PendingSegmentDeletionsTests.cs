using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Retention has to survive a segment file it cannot delete yet, and must not forget it.
///
/// <para>The situation is real: a fetch serves its bytes out of the segment, and with the File engine
/// that read can hold a memory-mapped view. Windows refuses to delete a mapped file, so the deletion
/// fails at exactly the moment retention wants it. Dropping it there is the dangerous outcome — the
/// partition has already moved past the segment, but the file survives on disk and comes back as
/// live records on the next start.</para>
///
/// <para>These tests use a segment that refuses deletion on demand, so the retry contract holds on
/// every platform — on Linux the real unlink succeeds immediately and would never exercise it.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PendingSegmentDeletionsTests
{
    [Fact]
    public void DeleteOrDefer_WhenFilesAreFree_DeletesImmediately()
    {
        var deletions = new PendingSegmentDeletions();
        var segment = new RefusingSegment(refusals: 0);

        Assert.True(deletions.DeleteOrDefer(segment));
        Assert.Equal(1, segment.DeleteAttempts);
        Assert.Equal(0, deletions.Count);
    }

    [Fact]
    public void DeleteOrDefer_WhenFileIsPinned_KeepsItForRetry()
    {
        var deletions = new PendingSegmentDeletions();
        var segment = new RefusingSegment(refusals: 1);

        // The partition drops the segment either way — what must not happen is the deletion being
        // forgotten, because then the file outlives the records it holds.
        Assert.False(deletions.DeleteOrDefer(segment));
        Assert.Equal(1, deletions.Count);
    }

    [Fact]
    public void RetryPending_DeletesOnceTheReaderIsGone()
    {
        var deletions = new PendingSegmentDeletions();
        var segment = new RefusingSegment(refusals: 1);

        deletions.DeleteOrDefer(segment);

        Assert.Equal(1, deletions.RetryPending());
        Assert.Equal(0, deletions.Count);
        Assert.Equal(2, segment.DeleteAttempts);
    }

    [Fact]
    public void RetryPending_KeepsRetryingWhileTheFileStaysPinned()
    {
        // A long-running fetch can outlive several retention passes; giving up after one retry would
        // reintroduce the very leak this class exists to prevent.
        var deletions = new PendingSegmentDeletions();
        var segment = new RefusingSegment(refusals: 3);

        deletions.DeleteOrDefer(segment);

        Assert.Equal(0, deletions.RetryPending());
        Assert.Equal(0, deletions.RetryPending());
        Assert.Equal(1, deletions.Count);

        Assert.Equal(1, deletions.RetryPending());
        Assert.Equal(0, deletions.Count);
    }

    [Fact]
    public void RetryPending_OnEmptyList_DoesNothing()
    {
        var deletions = new PendingSegmentDeletions();

        Assert.Equal(0, deletions.RetryPending());
        Assert.Equal(0, deletions.Count);
    }

    [Fact]
    public void RetryPending_KeepsTheUndeletableAndReleasesTheRest()
    {
        var deletions = new PendingSegmentDeletions();
        var freed = new RefusingSegment(refusals: 1);
        var stillPinned = new RefusingSegment(refusals: 99);

        deletions.DeleteOrDefer(freed);
        deletions.DeleteOrDefer(stillPinned);

        Assert.Equal(1, deletions.RetryPending());
        Assert.Equal(1, deletions.Count);
    }

    /// <summary>A segment whose files refuse deletion the first <c>refusals</c> times.</summary>
    private sealed class RefusingSegment(int refusals) : ILogSegment
    {
        private int _refusalsLeft = refusals;

        public int DeleteAttempts { get; private set; }

        public void DeleteFiles()
        {
            DeleteAttempts++;
            if (_refusalsLeft-- > 0)
            {
                // What Windows raises for a file that still has a mapped view.
                throw new IOException("The requested operation cannot be performed on a file with a user-mapped section open.");
            }
        }

        public long BaseOffset => 0;
        public long CurrentOffset => 0;
        public long Size => 0;
        public bool IsFull => false;
        public DateTime CreatedAt => DateTime.UtcNow;
        public long MaxTimestamp => 0;
        public long? GetFirstMessageOffset() => null;
        public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default)
            => ValueTask.FromResult((0L, 0));
        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new List<byte[]>());
        public ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
            => ValueTask.FromResult((ReadOnlyMemory<byte>.Empty, new List<int>()));
        public long? GetFilePositionForOffset(long startOffset) => null;
        public long? FindOffsetByTimestamp(long targetTimestamp) => null;
        public void Dispose() { }
    }
}

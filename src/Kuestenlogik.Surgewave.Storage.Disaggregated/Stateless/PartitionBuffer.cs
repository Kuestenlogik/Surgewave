using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Stateless;

/// <summary>
/// In-RAM per-partition buffer behind <see cref="StatelessAgent"/>.
/// Holds pending batches + their completion tickets until either the
/// size threshold or the age threshold triggers a flush. All public
/// methods are thread-safe.
/// </summary>
internal sealed class PartitionBuffer
{
    private readonly object _lock = new();
    private readonly List<PendingBatch> _pending = [];
    private long _pendingBytes;
    private DateTime _firstEnqueuedAt;

    public PartitionBuffer(TopicPartition partition)
    {
        Partition = partition;
    }

    public TopicPartition Partition { get; }

    public long PendingBytes
    {
        get
        {
            lock (_lock) return _pendingBytes;
        }
    }

    public PendingTicket Enqueue(byte[] bytes, int recordCount, DateTime utcNow)
    {
        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lock)
        {
            if (_pending.Count == 0) _firstEnqueuedAt = utcNow;
            _pending.Add(new PendingBatch(bytes, recordCount, tcs));
            _pendingBytes += bytes.LongLength;
        }
        return new PendingTicket(tcs, recordCount);
    }

    public bool IsExpired(TimeSpan maxAge, DateTime utcNow)
    {
        lock (_lock)
        {
            if (_pending.Count == 0) return false;
            return utcNow - _firstEnqueuedAt >= maxAge;
        }
    }

    /// <summary>
    /// Take everything currently buffered. Returns null when the buffer
    /// is empty (concurrent flush already drained it). The caller is
    /// responsible for completing each ticket after the upload + commit
    /// succeed (or faulting them on failure).
    /// </summary>
    public BufferSnapshot? ExtractForFlush()
    {
        lock (_lock)
        {
            if (_pending.Count == 0) return null;

            var totalRecords = 0;
            var totalBytes = 0L;
            foreach (var p in _pending)
            {
                totalRecords += p.RecordCount;
                totalBytes += p.Bytes.LongLength;
            }
            var combined = new byte[totalBytes];
            var cursor = 0;
            var tickets = new List<PendingTicket>(_pending.Count);
            foreach (var p in _pending)
            {
                Array.Copy(p.Bytes, 0, combined, cursor, p.Bytes.Length);
                cursor += p.Bytes.Length;
                tickets.Add(new PendingTicket(p.Tcs, p.RecordCount));
            }
            _pending.Clear();
            _pendingBytes = 0;
            return new BufferSnapshot(combined, totalRecords, tickets);
        }
    }
}

internal readonly record struct PendingBatch(byte[] Bytes, int RecordCount, TaskCompletionSource<long> Tcs);

internal readonly record struct PendingTicket(TaskCompletionSource<long> Tcs, int RecordCount)
{
    public Task<long> Task => Tcs.Task;
    public bool TrySetCanceled(CancellationToken ct) => Tcs.TrySetCanceled(ct);
}

internal readonly record struct BufferSnapshot(byte[] LogBytes, int RecordCount, IReadOnlyList<PendingTicket> Tickets);

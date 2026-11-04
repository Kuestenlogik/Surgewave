using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// One share group's view of one partition (KIP-932), held by the partition's leader (#228): which records the group's
/// members hold, which wait for a member, which are done. Records from the share-partition start offset (SPSO) up to the
/// end offset (SPEO) are in flight, each <see cref="RecordState.Available"/>, <see cref="RecordState.Acquired"/> by a
/// member until its lock runs out, <see cref="RecordState.Acknowledged"/> or <see cref="RecordState.Archived"/>; the
/// start offset moves past the records that are done.
/// </summary>
/// <remarks>
/// Records are the log's records, one offset each — a batch of several records is several offsets. A record that has
/// been delivered as often as the group allows is archived instead of being offered again, as is one a member rejects.
/// </remarks>
internal sealed class SharePartition
{
    private readonly IPartitionLog _log;
    private readonly TimeProvider _time;
    private readonly TimeSpan _lockDuration;
    private readonly Lock _lock = new();
    private readonly SortedDictionary<long, InFlightRecord> _records = [];

    public SharePartition(IPartitionLog log, long startOffset, TimeSpan lockDuration, TimeProvider? timeProvider = null)
    {
        _log = log;
        _lockDuration = lockDuration;
        _time = timeProvider ?? TimeProvider.System;
        StartOffset = Math.Max(startOffset, log.LogStartOffset);
        EndOffset = StartOffset;
    }

    /// <summary>The share-partition start offset: every record below it is done.</summary>
    public long StartOffset { get; private set; }

    /// <summary>The share-partition end offset: no record from here on has been offered yet.</summary>
    public long EndOffset { get; private set; }

    /// <summary>The records a member holds or that wait for one.</summary>
    public int InFlightCount
    {
        get
        {
            lock (_lock)
                return _records.Values.Count(r => r.State is RecordState.Acquired or RecordState.Available);
        }
    }

    /// <summary>
    /// Gives <paramref name="memberId"/> up to <paramref name="maxRecords"/> records: those waiting for a member first,
    /// then records new in the log up to its high watermark — while the group holds fewer than
    /// <paramref name="maxRecordLocks"/> records. Returns the acquired offset ranges and the batches that hold them.
    /// </summary>
    public async Task<ShareAcquisition> AcquireAsync(
        string memberId, int maxRecords, int deliveryLimit, int maxRecordLocks, CancellationToken cancellationToken)
    {
        var acquired = new List<(long Offset, short DeliveryCount)>();
        long readFrom;
        int budget;
        lock (_lock)
        {
            ExpireLocks(deliveryLimit);
            var now = _time.GetUtcNow();
            foreach (var (offset, record) in _records)
            {
                if (acquired.Count >= maxRecords)
                    break;
                if (record.State != RecordState.Available)
                    continue;
                Acquire(record, memberId, now);
                acquired.Add((offset, record.DeliveryCount));
            }
            readFrom = EndOffset;
            budget = Math.Min(maxRecords - acquired.Count, maxRecordLocks - LockedCount());
        }

        if (budget > 0 && readFrom < _log.HighWatermark)
        {
            var batches = await _log.ReadBatchesAsync(readFrom, maxBytes: 1_048_576, cancellationToken).ConfigureAwait(false);
            lock (_lock)
            {
                var now = _time.GetUtcNow();
                var highWatermark = _log.HighWatermark;
                foreach (var batch in batches)
                {
                    if (budget <= 0)
                        break;
                    var (baseOffset, lastOffset) = Range(batch);
                    var isControl = CompressionCodec.IsControlBatch(batch);
                    // Another acquisition may have taken these offsets in since the read began.
                    for (var offset = Math.Max(baseOffset, EndOffset); offset <= lastOffset && offset < highWatermark; offset++)
                    {
                        if (isControl)
                        {
                            // A transaction marker is no record for a member: done as it is offered.
                            _records[offset] = new InFlightRecord { State = RecordState.Archived };
                        }
                        else
                        {
                            if (budget <= 0)
                                break;
                            var record = new InFlightRecord();
                            Acquire(record, memberId, now);
                            _records[offset] = record;
                            acquired.Add((offset, record.DeliveryCount));
                            budget--;
                        }
                        EndOffset = offset + 1;
                    }
                }
                AdvanceStart();
            }
        }

        if (acquired.Count == 0)
            return ShareAcquisition.Empty;
        acquired.Sort((a, b) => a.Offset.CompareTo(b.Offset));
        return new ShareAcquisition(Ranges(acquired), await BatchesHoldingAsync(acquired, cancellationToken).ConfigureAwait(false));
    }

    /// <summary>
    /// Applies a member's acknowledgements, by offset range, each range with one type per offset or one for all — 0 gap,
    /// 1 accept, 2 release, 3 reject, 4 renew. Only records the member holds can be acknowledged; nothing is applied
    /// when one of them is not.
    /// </summary>
    public ShareGroupAckOutcome Acknowledge(string? memberId, IReadOnlyList<(long FirstOffset, long LastOffset, IReadOnlyList<sbyte> Types)> batches, int deliveryLimit)
    {
        lock (_lock)
        {
            ExpireLocks(deliveryLimit);
            foreach (var (first, last, types) in batches)
            {
                for (var offset = first; offset <= last; offset++)
                {
                    var type = types.Count == 0 ? (sbyte)1 : types[(int)Math.Min(offset - first, types.Count - 1)];
                    if (type == 0)
                        continue; // a gap: no record at this offset
                    if (!_records.TryGetValue(offset, out var record) || record.State != RecordState.Acquired
                        || (memberId is not null && record.MemberId != memberId))
                    {
                        return ShareGroupAckOutcome.InvalidRecordState;
                    }
                }
            }

            var now = _time.GetUtcNow();
            foreach (var (first, last, types) in batches)
            {
                for (var offset = first; offset <= last; offset++)
                {
                    var type = types.Count == 0 ? (sbyte)1 : types[(int)Math.Min(offset - first, types.Count - 1)];
                    if (!_records.TryGetValue(offset, out var record))
                        continue;
                    switch (type)
                    {
                        case 0:
                            record.State = RecordState.Archived;
                            break;
                        case 1:
                            record.State = RecordState.Acknowledged;
                            break;
                        case 2:
                            Release(record, deliveryLimit);
                            break;
                        case 3:
                            record.State = RecordState.Archived;
                            break;
                        case 4:
                            record.LockedUntil = now + _lockDuration;
                            break;
                    }
                }
            }
            AdvanceStart();
            return ShareGroupAckOutcome.Ok;
        }
    }

    /// <summary>The records a member holds go back to the group — the member left or was fenced.</summary>
    public void ReleaseAll(string memberId, int deliveryLimit)
    {
        lock (_lock)
        {
            foreach (var record in _records.Values.Where(r => r.State == RecordState.Acquired && r.MemberId == memberId))
                Release(record, deliveryLimit);
            AdvanceStart();
        }
    }

    // Under the lock.
    private void Acquire(InFlightRecord record, string memberId, DateTimeOffset now)
    {
        record.State = RecordState.Acquired;
        record.MemberId = memberId;
        record.DeliveryCount++;
        record.LockedUntil = now + _lockDuration;
    }

    // Under the lock: back to the group, or archived once delivered as often as the group allows.
    private static void Release(InFlightRecord record, int deliveryLimit)
    {
        record.State = record.DeliveryCount >= deliveryLimit ? RecordState.Archived : RecordState.Available;
        record.MemberId = null;
    }

    // Under the lock: a lock that ran out releases its record.
    private void ExpireLocks(int deliveryLimit)
    {
        var now = _time.GetUtcNow();
        foreach (var record in _records.Values)
        {
            if (record.State == RecordState.Acquired && record.LockedUntil <= now)
                Release(record, deliveryLimit);
        }
        AdvanceStart();
    }

    private int LockedCount() => _records.Values.Count(r => r.State is RecordState.Acquired or RecordState.Available);

    // Under the lock: the start offset moves past every record that is done; an offset without a record — compacted
    // away — is done too.
    private void AdvanceStart()
    {
        while (StartOffset < EndOffset)
        {
            if (_records.TryGetValue(StartOffset, out var record))
            {
                if (record.State is not (RecordState.Acknowledged or RecordState.Archived))
                    break;
                _records.Remove(StartOffset);
            }
            StartOffset++;
        }
    }

    // A batch's first and last offset, from its header.
    private static (long Base, long Last) Range(byte[] batch)
    {
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(batch);
        var lastDelta = BinaryPrimitives.ReadInt32BigEndian(batch.AsSpan(KafkaConstants.RecordBatch.LastOffsetDeltaOffset));
        return (baseOffset, baseOffset + lastDelta);
    }

    // Consecutive offsets of one delivery count form one range, as ShareFetch's AcquiredRecords carry them.
    private static List<ShareAcquiredRange> Ranges(List<(long Offset, short DeliveryCount)> acquired)
    {
        var ranges = new List<ShareAcquiredRange>();
        var (first, last, count) = (acquired[0].Offset, acquired[0].Offset, acquired[0].DeliveryCount);
        foreach (var (offset, deliveryCount) in acquired.Skip(1))
        {
            if (offset == last + 1 && deliveryCount == count)
            {
                last = offset;
                continue;
            }
            ranges.Add(new ShareAcquiredRange(first, last, count));
            (first, last, count) = (offset, offset, deliveryCount);
        }
        ranges.Add(new ShareAcquiredRange(first, last, count));
        return ranges;
    }

    // The log's batches that hold the acquired offsets, in order; the client takes the acquired records out of them.
    private async Task<byte[]> BatchesHoldingAsync(List<(long Offset, short DeliveryCount)> acquired, CancellationToken cancellationToken)
    {
        var wanted = new HashSet<long>(acquired.Select(a => a.Offset));
        var last = acquired[^1].Offset;
        var position = acquired[0].Offset;
        var holding = new List<byte[]>();
        while (position <= last)
        {
            var batches = await _log.ReadBatchesAsync(position, maxBytes: 1_048_576, cancellationToken).ConfigureAwait(false);
            if (batches.Count == 0)
                break;
            var before = position;
            foreach (var batch in batches)
            {
                var (baseOffset, lastOffset) = Range(batch);
                if (lastOffset < position)
                    continue;
                if (baseOffset > last)
                {
                    position = last + 1;
                    break;
                }
                var holds = false;
                for (var o = baseOffset; o <= lastOffset && !holds; o++)
                    holds = wanted.Contains(o);
                if (holds)
                    holding.Add(batch);
                position = lastOffset + 1;
            }
            if (position <= before)
                break;
        }

        var bytes = new byte[holding.Sum(b => b.Length)];
        var at = 0;
        foreach (var batch in holding)
        {
            Buffer.BlockCopy(batch, 0, bytes, at, batch.Length);
            at += batch.Length;
        }
        return bytes;
    }

    private enum RecordState
    {
        Available,
        Acquired,
        Acknowledged,
        Archived,
    }

    private sealed class InFlightRecord
    {
        public RecordState State { get; set; } = RecordState.Available;
        public short DeliveryCount { get; set; }
        public string? MemberId { get; set; }
        public DateTimeOffset LockedUntil { get; set; }
    }
}

/// <summary>Records a member acquired: their offset ranges and the batches of the log that hold them.</summary>
internal sealed record ShareAcquisition(IReadOnlyList<ShareAcquiredRange> Ranges, byte[]? Batches)
{
    public static ShareAcquisition Empty { get; } = new([], null);
}

/// <summary>Consecutive acquired offsets of one delivery count.</summary>
internal readonly record struct ShareAcquiredRange(long FirstOffset, long LastOffset, short DeliveryCount);

/// <summary>How a member's acknowledgements went.</summary>
internal enum ShareGroupAckOutcome
{
    Ok,

    /// <summary>A record is not one the member holds (Kafka's INVALID_RECORD_STATE).</summary>
    InvalidRecordState,
}

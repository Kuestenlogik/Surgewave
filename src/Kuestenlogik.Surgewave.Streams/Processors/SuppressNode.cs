using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Streams.EventTime;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node that suppresses (buffers) updates for a configurable duration.
/// Used to reduce the number of updates emitted downstream, especially for aggregations.
/// </summary>
internal sealed class SuppressNode<TKey, TValue> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly Suppressed<TKey> _suppressed;
    private readonly string _storeName;

    private readonly ConcurrentDictionary<TKey, BufferedRecord> _buffer = new();
    private long _bufferSizeBytes;
    private long _lastEmitTime;
    private ICancellable? _punctuationHandle;

    public SuppressNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        Suppressed<TKey> suppressed,
        string storeName)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _suppressed = suppressed;
        _storeName = storeName;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _lastEmitTime = context.CurrentSystemTimeMs();

        // Schedule punctuation to check for expired records
        var checkInterval = _suppressed.BufferTime ?? TimeSpan.FromSeconds(1);
        _punctuationHandle = context.Schedule(
            checkInterval,
            PunctuationType.WallClockTime,
            OnPunctuate);
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var deserializedKey = _keySerde.Deserialize(key);
        var recordSize = key.Length + value.Length;

        // Update or insert the record in buffer
        _buffer.AddOrUpdate(
            deserializedKey,
            _ =>
            {
                _bufferSizeBytes += recordSize;
                return new BufferedRecord(key, value, timestamp, recordSize);
            },
            (_, existing) =>
            {
                // Replace existing record, adjust size
                _bufferSizeBytes = _bufferSizeBytes - existing.SizeBytes + recordSize;
                return new BufferedRecord(key, value, timestamp, recordSize);
            });

        // Check if we need to emit due to buffer size limit
        if (_suppressed.BufferSize.HasValue && _bufferSizeBytes >= _suppressed.BufferSize.Value)
        {
            EmitOldestRecords(1);
        }
    }

    private void OnPunctuate(long timestamp)
    {
        if (_suppressed.UntilWindowCloses)
        {
            // Emit all records whose windows have closed
            EmitClosedWindowRecords(timestamp);
        }
        else if (_suppressed.BufferTime.HasValue)
        {
            // Emit records older than the buffer time
            var cutoff = timestamp - (long)_suppressed.BufferTime.Value.TotalMilliseconds;
            EmitRecordsOlderThan(cutoff);
        }
    }

    private void EmitClosedWindowRecords(long currentTime)
    {
        var graceMs = (long)(_suppressed.BufferTime?.TotalMilliseconds ?? 0);

        // Use watermark for window-close detection if available
        var watermark = Context?.CurrentWatermark ?? EventTime.Watermark.None;
        var effectiveTime = !watermark.IsNone ? watermark.Timestamp : currentTime;

        var toEmit = _buffer
            .Where(kvp => kvp.Value.Timestamp + graceMs < effectiveTime)
            .ToList();

        foreach (var kvp in toEmit)
        {
            if (_buffer.TryRemove(kvp.Key, out var record))
            {
                _bufferSizeBytes -= record.SizeBytes;
                ForwardToChildren(record.Key, record.Value, record.Timestamp);
                Context?.Metrics.RecordProcessed(record.SizeBytes);
            }
        }
    }

    private void EmitRecordsOlderThan(long cutoffTimestamp)
    {
        var toEmit = _buffer
            .Where(kvp => kvp.Value.Timestamp < cutoffTimestamp)
            .ToList();

        foreach (var kvp in toEmit)
        {
            if (_buffer.TryRemove(kvp.Key, out var record))
            {
                _bufferSizeBytes -= record.SizeBytes;
                ForwardToChildren(record.Key, record.Value, record.Timestamp);
                Context?.Metrics.RecordProcessed(record.SizeBytes);
            }
        }
    }

    private void EmitOldestRecords(int count)
    {
        var oldest = _buffer
            .OrderBy(kvp => kvp.Value.Timestamp)
            .Take(count)
            .ToList();

        foreach (var kvp in oldest)
        {
            if (_buffer.TryRemove(kvp.Key, out var record))
            {
                _bufferSizeBytes -= record.SizeBytes;
                ForwardToChildren(record.Key, record.Value, record.Timestamp);
                Context?.Metrics.RecordProcessed(record.SizeBytes);
            }
        }
    }

    public override void Close()
    {
        _punctuationHandle?.Cancel();

        // Emit all remaining buffered records
        foreach (var kvp in _buffer)
        {
            ForwardToChildren(kvp.Value.Key, kvp.Value.Value, kvp.Value.Timestamp);
        }

        _buffer.Clear();
        _bufferSizeBytes = 0;
    }

    private readonly record struct BufferedRecord(byte[] Key, byte[] Value, long Timestamp, int SizeBytes);
}

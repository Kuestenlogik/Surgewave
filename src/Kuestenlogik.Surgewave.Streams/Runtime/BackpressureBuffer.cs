using System.Threading.Channels;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Record buffered for backpressure.
/// </summary>
public readonly record struct BufferedRecord(
    byte[]? Key,
    byte[]? Value,
    string Topic,
    int Partition,
    long Offset,
    long Timestamp);

/// <summary>
/// Bounded buffer with backpressure support between poll loop and processing.
/// </summary>
public sealed class BackpressureBuffer : IDisposable
{
    private readonly Channel<BufferedRecord> _channel;
    private readonly BackpressureConfig _config;
    private readonly StreamsMetrics _metrics;
    private readonly int _highWatermark;
    private readonly int _lowWatermark;
    private long _droppedCount;
    private bool _disposed;

    public int CurrentSize => _channel.Reader.Count;
    public long DroppedCount => Interlocked.Read(ref _droppedCount);
    public bool IsAboveHighWatermark => CurrentSize >= _highWatermark;
    public bool IsBelowLowWatermark => CurrentSize <= _lowWatermark;

    public BackpressureBuffer(BackpressureConfig config, StreamsMetrics metrics)
    {
        _config = config;
        _metrics = metrics;
        _highWatermark = (int)(config.MaxBufferedRecords * config.HighWatermarkRatio);
        _lowWatermark = (int)(config.MaxBufferedRecords * config.LowWatermarkRatio);

        var options = config.Strategy == BackpressureStrategy.Block
            ? new BoundedChannelOptions(config.MaxBufferedRecords)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            }
            : new BoundedChannelOptions(config.MaxBufferedRecords)
            {
                FullMode = config.Strategy == BackpressureStrategy.DropOldest
                    ? BoundedChannelFullMode.DropOldest
                    : BoundedChannelFullMode.DropNewest,
                SingleReader = true,
                SingleWriter = true
            };

        _channel = Channel.CreateBounded<BufferedRecord>(options);
    }

    /// <summary>
    /// Writes a record to the buffer. Behavior depends on strategy.
    /// Returns true if the record was accepted, false if dropped.
    /// </summary>
    public async ValueTask<bool> WriteAsync(BufferedRecord record, CancellationToken cancellationToken = default)
    {
        if (_config.Strategy == BackpressureStrategy.Block)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_config.MaxWaitTime);

            try
            {
                await _channel.Writer.WriteAsync(record, cts.Token);
                return true;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // MaxWaitTime exceeded
                Interlocked.Increment(ref _droppedCount);
                _metrics.RecordBackpressureDrop();
                return false;
            }
        }

        if (_channel.Writer.TryWrite(record))
            return true;

        // For DropOldest/DropNewest the channel handles it, but TryWrite can still fail
        Interlocked.Increment(ref _droppedCount);
        _metrics.RecordBackpressureDrop();
        return false;
    }

    /// <summary>
    /// Reads the next record from the buffer.
    /// </summary>
    public ValueTask<BufferedRecord> ReadAsync(CancellationToken cancellationToken = default)
    {
        return _channel.Reader.ReadAsync(cancellationToken);
    }

    /// <summary>
    /// Tries to read a record without blocking.
    /// </summary>
    public bool TryRead(out BufferedRecord record)
    {
        return _channel.Reader.TryRead(out record);
    }

    /// <summary>
    /// Signals that no more records will be written.
    /// </summary>
    public void Complete()
    {
        _channel.Writer.TryComplete();
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        Complete();
        _disposed = true;
    }
}

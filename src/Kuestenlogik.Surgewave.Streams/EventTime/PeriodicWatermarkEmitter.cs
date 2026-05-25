using System.Threading.Channels;

namespace Kuestenlogik.Surgewave.Streams.EventTime;

/// <summary>
/// Emits watermarks periodically for a stream.
/// </summary>
public sealed class PeriodicWatermarkEmitter<T> : IAsyncDisposable
{
    private readonly IWatermarkGenerator<T> _generator;
    private readonly TimeSpan _interval;
    private readonly Channel<Watermark> _watermarkChannel;
    private readonly CancellationTokenSource _cts = new();
    private Task? _emitTask;
    private Watermark _lastEmitted = Watermark.None;

    public PeriodicWatermarkEmitter(
        IWatermarkGenerator<T> generator,
        TimeSpan interval,
        int channelCapacity = 100)
    {
        _generator = generator;
        _interval = interval;
        _watermarkChannel = Channel.CreateBounded<Watermark>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });
    }

    /// <summary>
    /// Gets the channel reader for watermarks.
    /// </summary>
    public ChannelReader<Watermark> Watermarks => _watermarkChannel.Reader;

    /// <summary>
    /// Gets the last emitted watermark.
    /// </summary>
    public Watermark LastEmittedWatermark => _lastEmitted;

    /// <summary>
    /// Processes an event, updating the watermark generator.
    /// </summary>
    public void OnEvent(T element, long eventTimestamp)
    {
        _generator.OnEvent(element, eventTimestamp);
    }

    /// <summary>
    /// Starts the periodic watermark emission.
    /// </summary>
    public void Start()
    {
        _emitTask = EmitLoop();
    }

    private async Task EmitLoop()
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                await Task.Delay(_interval, _cts.Token);

                var watermark = _generator.GetCurrentWatermark();
                if (watermark > _lastEmitted && !watermark.IsNone)
                {
                    _lastEmitted = watermark;
                    await _watermarkChannel.Writer.WriteAsync(watermark, _cts.Token);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
    }

    /// <summary>
    /// Emits a final watermark (end of stream).
    /// </summary>
    public async Task EmitFinalWatermarkAsync()
    {
        await _watermarkChannel.Writer.WriteAsync(Watermark.Max);
        _watermarkChannel.Writer.Complete();
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_emitTask != null)
        {
            try { await _emitTask; }
            catch (OperationCanceledException) { }
        }
        _watermarkChannel.Writer.TryComplete();
        _cts.Dispose();
    }
}

/// <summary>
/// Tracks watermarks across multiple partitions/sources.
/// </summary>
public sealed class WatermarkTracker
{
    private readonly Dictionary<int, Watermark> _partitionWatermarks = new();
    private readonly object _lock = new();

    /// <summary>
    /// Updates the watermark for a partition.
    /// </summary>
    public void UpdateWatermark(int partition, Watermark watermark)
    {
        lock (_lock)
        {
            if (!_partitionWatermarks.TryGetValue(partition, out var current) || watermark > current)
            {
                _partitionWatermarks[partition] = watermark;
            }
        }
    }

    /// <summary>
    /// Gets the combined watermark (minimum across all partitions).
    /// </summary>
    public Watermark GetCombinedWatermark()
    {
        lock (_lock)
        {
            if (_partitionWatermarks.Count == 0)
                return Watermark.None;

            return _partitionWatermarks.Values.Min();
        }
    }

    /// <summary>
    /// Marks a partition as idle (sets its watermark to max).
    /// </summary>
    public void MarkIdle(int partition)
    {
        lock (_lock)
        {
            _partitionWatermarks[partition] = Watermark.Max;
        }
    }

    /// <summary>
    /// Removes a partition from tracking.
    /// </summary>
    public void RemovePartition(int partition)
    {
        lock (_lock)
        {
            _partitionWatermarks.Remove(partition);
        }
    }

    /// <summary>
    /// Gets watermarks for all partitions.
    /// </summary>
    public IReadOnlyDictionary<int, Watermark> GetAllWatermarks()
    {
        lock (_lock)
        {
            return new Dictionary<int, Watermark>(_partitionWatermarks);
        }
    }
}

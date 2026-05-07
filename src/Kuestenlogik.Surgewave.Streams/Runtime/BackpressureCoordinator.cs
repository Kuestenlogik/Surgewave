namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Connects buffer fill level to consumer pause/resume decisions.
/// Fires events when high/low watermarks are crossed and tracks paused state.
/// </summary>
public sealed class BackpressureCoordinator
{
    private readonly BackpressureConfig _config;
    private readonly int _highWatermark;
    private readonly int _lowWatermark;
    private int _isPaused; // 0 = not paused, 1 = paused (int for Interlocked)

    /// <summary>Raised when the buffer crosses the high watermark (consumer should pause).</summary>
    public event Action? OnHighWatermarkReached;

    /// <summary>Raised when the buffer drops below the low watermark (consumer can resume).</summary>
    public event Action? OnLowWatermarkReached;

    /// <summary>Whether the consumer is currently paused due to backpressure.</summary>
    public bool IsPaused => Volatile.Read(ref _isPaused) == 1;

    public BackpressureCoordinator(BackpressureConfig config)
    {
        _config = config;
        _highWatermark = (int)(config.MaxBufferedRecords * config.HighWatermarkRatio);
        _lowWatermark = (int)(config.MaxBufferedRecords * config.LowWatermarkRatio);
    }

    /// <summary>
    /// Called after a record is written to the buffer.
    /// Pauses the consumer when the high watermark is reached.
    /// </summary>
    public void OnRecordBuffered(int currentBufferSize)
    {
        if (!_config.PauseConsumerOnHighWatermark)
            return;

        if (currentBufferSize >= _highWatermark &&
            Interlocked.CompareExchange(ref _isPaused, 1, 0) == 0)
        {
            OnHighWatermarkReached?.Invoke();
        }
    }

    /// <summary>
    /// Called after a record is consumed from the buffer.
    /// Resumes the consumer when the buffer drops below the low watermark.
    /// </summary>
    public void OnRecordProcessed(int currentBufferSize)
    {
        if (currentBufferSize <= _lowWatermark &&
            Interlocked.CompareExchange(ref _isPaused, 0, 1) == 1)
        {
            OnLowWatermarkReached?.Invoke();
        }
    }

    /// <summary>
    /// Resets the coordinator state (e.g. after partition revocation).
    /// </summary>
    public void Reset()
    {
        Interlocked.Exchange(ref _isPaused, 0);
    }
}

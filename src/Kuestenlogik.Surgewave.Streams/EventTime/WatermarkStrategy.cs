namespace Kuestenlogik.Surgewave.Streams.EventTime;

/// <summary>
/// Strategy for generating watermarks and assigning timestamps.
/// Provides a Flink-style fluent API for watermark configuration.
/// </summary>
public sealed class WatermarkStrategy<T>
{
    private readonly Func<IWatermarkGenerator<T>> _generatorFactory;
    private ITimestampAssigner<T>? _timestampAssigner;
    private TimeSpan _idleTimeout = TimeSpan.Zero;

    private WatermarkStrategy(Func<IWatermarkGenerator<T>> generatorFactory)
    {
        _generatorFactory = generatorFactory;
    }

    /// <summary>
    /// Creates a watermark strategy for event time with bounded out-of-orderness.
    /// Events may arrive up to maxOutOfOrderness late.
    /// </summary>
    public static WatermarkStrategy<T> ForBoundedOutOfOrderness(TimeSpan maxOutOfOrderness)
    {
        return new WatermarkStrategy<T>(() => new BoundedOutOfOrdernessGenerator<T>(maxOutOfOrderness));
    }

    /// <summary>
    /// Creates a watermark strategy for perfectly ordered events (no late events).
    /// </summary>
    public static WatermarkStrategy<T> ForMonotonousTimestamps()
    {
        return new WatermarkStrategy<T>(() => new AscendingTimestampGenerator<T>());
    }

    /// <summary>
    /// Creates a watermark strategy that does not generate watermarks.
    /// Use for processing time semantics.
    /// </summary>
    public static WatermarkStrategy<T> NoWatermarks()
    {
        return new WatermarkStrategy<T>(() => new NoWatermarksGenerator<T>());
    }

    /// <summary>
    /// Creates a custom watermark strategy.
    /// </summary>
    public static WatermarkStrategy<T> ForGenerator(Func<IWatermarkGenerator<T>> generatorFactory)
    {
        return new WatermarkStrategy<T>(generatorFactory);
    }

    /// <summary>
    /// Specifies how to extract the event timestamp from elements.
    /// </summary>
    public WatermarkStrategy<T> WithTimestampAssigner(ITimestampAssigner<T> assigner)
    {
        _timestampAssigner = assigner;
        return this;
    }

    /// <summary>
    /// Specifies how to extract the event timestamp from elements.
    /// </summary>
    public WatermarkStrategy<T> WithTimestampAssigner(Func<T, long, long> extractor)
    {
        _timestampAssigner = new LambdaTimestampAssigner<T>(extractor);
        return this;
    }

    /// <summary>
    /// Configures the strategy to emit a watermark when a source becomes idle.
    /// </summary>
    public WatermarkStrategy<T> WithIdleness(TimeSpan idleTimeout)
    {
        _idleTimeout = idleTimeout;
        return this;
    }

    /// <summary>
    /// Creates a new watermark generator instance.
    /// </summary>
    public IWatermarkGenerator<T> CreateWatermarkGenerator()
    {
        return _generatorFactory();
    }

    /// <summary>
    /// Gets the timestamp assigner or a default one that uses the record timestamp.
    /// </summary>
    public ITimestampAssigner<T> GetTimestampAssigner()
    {
        return _timestampAssigner ?? new LambdaTimestampAssigner<T>((_, recordTimestamp) => recordTimestamp);
    }

    /// <summary>
    /// Gets the idle timeout configuration.
    /// </summary>
    public TimeSpan IdleTimeout => _idleTimeout;
}

/// <summary>
/// Generator for bounded out-of-orderness watermarks.
/// </summary>
internal sealed class BoundedOutOfOrdernessGenerator<T> : IWatermarkGenerator<T>
{
    private readonly long _maxOutOfOrdernessMs;
    private long _maxTimestamp = long.MinValue;

    public BoundedOutOfOrdernessGenerator(TimeSpan maxOutOfOrderness)
    {
        _maxOutOfOrdernessMs = (long)maxOutOfOrderness.TotalMilliseconds;
    }

    public void OnEvent(T element, long eventTimestamp)
    {
        if (eventTimestamp > _maxTimestamp)
        {
            _maxTimestamp = eventTimestamp;
        }
    }

    public Watermark GetCurrentWatermark()
    {
        if (_maxTimestamp == long.MinValue)
            return Watermark.None;

        return new Watermark(_maxTimestamp - _maxOutOfOrdernessMs - 1);
    }
}

/// <summary>
/// Generator for strictly ascending timestamps.
/// </summary>
internal sealed class AscendingTimestampGenerator<T> : IWatermarkGenerator<T>
{
    private long _maxTimestamp = long.MinValue;

    public void OnEvent(T element, long eventTimestamp)
    {
        if (eventTimestamp > _maxTimestamp)
        {
            _maxTimestamp = eventTimestamp;
        }
    }

    public Watermark GetCurrentWatermark()
    {
        if (_maxTimestamp == long.MinValue)
            return Watermark.None;

        return new Watermark(_maxTimestamp - 1);
    }
}

/// <summary>
/// Generator that never emits watermarks (processing time semantics).
/// </summary>
internal sealed class NoWatermarksGenerator<T> : IWatermarkGenerator<T>
{
    public void OnEvent(T element, long eventTimestamp) { }
    public Watermark GetCurrentWatermark() => Watermark.None;
}

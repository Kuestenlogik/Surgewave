namespace Kuestenlogik.Surgewave.Streams.Windows;

/// <summary>
/// Tumbling (fixed-size, non-overlapping) windows.
/// </summary>
public sealed class TumblingWindows : Windows
{
    private readonly long _sizeMs;

    public override long Size => _sizeMs;

    private TumblingWindows(TimeSpan size)
    {
        _sizeMs = (long)size.TotalMilliseconds;
    }

    public static TumblingWindows Of(TimeSpan size) => new(size);

    public TumblingWindows Grace(TimeSpan gracePeriod)
    {
        GracePeriod = gracePeriod;
        return this;
    }

    public override IEnumerable<Window> WindowsFor(long timestamp)
    {
        var windowStart = timestamp - (timestamp % _sizeMs);
        yield return new Window(windowStart, windowStart + _sizeMs);
    }
}

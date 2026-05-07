namespace Kuestenlogik.Surgewave.Streams.Windows;

/// <summary>
/// Sliding windows (for joins).
/// </summary>
public sealed class SlidingWindows : Windows
{
    private readonly long _timeDifferenceMs;

    public override long Size => _timeDifferenceMs;

    private SlidingWindows(TimeSpan timeDifference)
    {
        _timeDifferenceMs = (long)timeDifference.TotalMilliseconds;
    }

    public static SlidingWindows WithTimeDifference(TimeSpan timeDifference) => new(timeDifference);

    public SlidingWindows Grace(TimeSpan gracePeriod)
    {
        GracePeriod = gracePeriod;
        return this;
    }

    public override IEnumerable<Window> WindowsFor(long timestamp)
    {
        // Sliding windows create a unique window for each event
        yield return new Window(timestamp - _timeDifferenceMs, timestamp + _timeDifferenceMs);
    }
}

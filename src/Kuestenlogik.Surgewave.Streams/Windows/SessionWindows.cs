namespace Kuestenlogik.Surgewave.Streams.Windows;

/// <summary>
/// Session windows (gap-based).
/// </summary>
public sealed class SessionWindows : Windows
{
    private readonly long _inactivityGapMs;

    public override long Size => _inactivityGapMs;
    public long InactivityGap => _inactivityGapMs;

    private SessionWindows(TimeSpan inactivityGap)
    {
        _inactivityGapMs = (long)inactivityGap.TotalMilliseconds;
    }

    public static SessionWindows With(TimeSpan inactivityGap) => new(inactivityGap);

    public SessionWindows Grace(TimeSpan gracePeriod)
    {
        GracePeriod = gracePeriod;
        return this;
    }

    public override IEnumerable<Window> WindowsFor(long timestamp)
    {
        // Session windows are dynamic, so we return a point window
        // The actual session window is computed by the session store
        yield return new Window(timestamp, timestamp);
    }
}

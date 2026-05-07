namespace Kuestenlogik.Surgewave.Streams.Windows;

/// <summary>
/// Time windows specification for joins.
/// </summary>
public sealed class JoinWindows : Windows
{
    private readonly long _beforeMs;
    private readonly long _afterMs;

    public override long Size => _beforeMs + _afterMs;
    public long BeforeMs => _beforeMs;
    public long AfterMs => _afterMs;

    private JoinWindows(TimeSpan timeDifference)
    {
        _beforeMs = (long)timeDifference.TotalMilliseconds;
        _afterMs = (long)timeDifference.TotalMilliseconds;
    }

    public static JoinWindows Of(TimeSpan timeDifference) => new(timeDifference);

    public JoinWindows Before(TimeSpan before)
    {
        return new JoinWindows(TimeSpan.FromMilliseconds(_afterMs))
        {
            GracePeriod = GracePeriod
        };
    }

    public JoinWindows After(TimeSpan after)
    {
        return new JoinWindows(TimeSpan.FromMilliseconds(_beforeMs))
        {
            GracePeriod = GracePeriod
        };
    }

    public JoinWindows Grace(TimeSpan gracePeriod)
    {
        GracePeriod = gracePeriod;
        return this;
    }

    public override IEnumerable<Window> WindowsFor(long timestamp)
    {
        yield return new Window(timestamp - _beforeMs, timestamp + _afterMs);
    }
}

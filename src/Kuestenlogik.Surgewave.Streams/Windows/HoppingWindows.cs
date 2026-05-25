namespace Kuestenlogik.Surgewave.Streams.Windows;

/// <summary>
/// Hopping (fixed-size, overlapping) windows.
/// </summary>
public sealed class HoppingWindows : Windows
{
    private readonly long _sizeMs;
    private readonly long _advanceMs;

    public override long Size => _sizeMs;
    public long Advance => _advanceMs;

    private HoppingWindows(TimeSpan size, TimeSpan advance)
    {
        _sizeMs = (long)size.TotalMilliseconds;
        _advanceMs = (long)advance.TotalMilliseconds;
    }

    public static HoppingWindows Of(TimeSpan size) => new(size, size);

    public HoppingWindows AdvanceBy(TimeSpan advance)
    {
        return new HoppingWindows(TimeSpan.FromMilliseconds(_sizeMs), advance)
        {
            GracePeriod = GracePeriod
        };
    }

    public HoppingWindows Grace(TimeSpan gracePeriod)
    {
        GracePeriod = gracePeriod;
        return this;
    }

    public override IEnumerable<Window> WindowsFor(long timestamp)
    {
        // Calculate all windows that contain this timestamp
        var windowStart = timestamp - (timestamp % _advanceMs);

        // Go back to find all windows that could contain this timestamp
        var earliestWindowStart = Math.Max(0, windowStart - _sizeMs + _advanceMs);

        for (var start = earliestWindowStart; start <= windowStart; start += _advanceMs)
        {
            if (timestamp >= start && timestamp < start + _sizeMs)
            {
                yield return new Window(start, start + _sizeMs);
            }
        }
    }
}

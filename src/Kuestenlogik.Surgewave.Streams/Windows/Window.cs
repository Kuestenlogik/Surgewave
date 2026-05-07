namespace Kuestenlogik.Surgewave.Streams.Windows;

/// <summary>
/// Time window definition.
/// </summary>
public readonly record struct Window(long StartMs, long EndMs)
{
    public TimeSpan Duration => TimeSpan.FromMilliseconds(EndMs - StartMs);
}

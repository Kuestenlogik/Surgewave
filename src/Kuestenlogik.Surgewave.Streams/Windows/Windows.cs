namespace Kuestenlogik.Surgewave.Streams.Windows;

/// <summary>
/// Base class for window specifications.
/// </summary>
public abstract class Windows
{
    public abstract long Size { get; }
    public TimeSpan GracePeriod { get; protected set; } = TimeSpan.Zero;

    public abstract IEnumerable<Window> WindowsFor(long timestamp);
}

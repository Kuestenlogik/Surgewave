namespace Kuestenlogik.Surgewave.Samples.BrokerPlugin;

/// <summary>
/// Thread-safe counter the sample plugin exposes via DI. Real plugins
/// would surface counters through <c>System.Diagnostics.Metrics.Meter</c>
/// so they show up alongside the broker's OpenTelemetry pipeline; the
/// raw <see cref="long"/> here keeps the example focused on the plugin
/// lifecycle.
/// </summary>
public sealed class RequestCounter
{
    private long _count;

    public long Count => Interlocked.Read(ref _count);

    public void Increment() => Interlocked.Increment(ref _count);
}

namespace Kuestenlogik.Surgewave.Broker.Tests.Fakes;

/// <summary>
/// Shared counter for the leases handed out by <see cref="LeaseTrackingLogSegment"/>.
/// </summary>
internal sealed class LeaseTracker
{
    private int _openLeases;

    public int OpenLeases => Volatile.Read(ref _openLeases);

    public void Acquired() => Interlocked.Increment(ref _openLeases);

    public void Released() => Interlocked.Decrement(ref _openLeases);
}

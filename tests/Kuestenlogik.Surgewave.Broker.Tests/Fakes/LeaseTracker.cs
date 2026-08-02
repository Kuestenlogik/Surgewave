namespace Kuestenlogik.Surgewave.Broker.Tests.Fakes;

/// <summary>
/// Shared counter for the leases handed out by <see cref="LeaseTrackingLogSegment"/>.
/// </summary>
internal sealed class LeaseTracker
{
    private int _openLeases;
    private int _doubleReleases;

    public int OpenLeases => Volatile.Read(ref _openLeases);

    /// <summary>
    /// How often a lease was released a second time. A real pool would hand the same buffer to two
    /// callers at that point, so this must stay zero — the fake counts it instead of quietly
    /// absorbing it, otherwise a response that releases twice looks identical to a correct one.
    /// </summary>
    public int DoubleReleases => Volatile.Read(ref _doubleReleases);

    public void Acquired() => Interlocked.Increment(ref _openLeases);

    public void Released() => Interlocked.Decrement(ref _openLeases);

    public void DoubleReleased() => Interlocked.Increment(ref _doubleReleases);
}

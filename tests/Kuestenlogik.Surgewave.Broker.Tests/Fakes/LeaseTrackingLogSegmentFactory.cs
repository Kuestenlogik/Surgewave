using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Broker.Tests.Fakes;

/// <summary>
/// Wraps another factory so every segment it creates serves contiguous reads out of a tracked
/// lease (#78). Lets a test observe what a real pooled or memory-mapped engine does but a plain
/// in-memory segment does not: hand memory out on loan and take it back on release.
/// </summary>
internal sealed class LeaseTrackingLogSegmentFactory(ILogSegmentFactory inner) : ILogSegmentFactory
{
    private readonly LeaseTracker _tracker = new();

    /// <summary>Leases handed out and not yet released.</summary>
    public int OpenLeases => _tracker.OpenLeases;

    public bool IsPersistent => inner.IsPersistent;

    public ILogSegment CreateSegment(string baseDirectory, long baseOffset, bool createNew, long maxSegmentSize = ILogSegment.DefaultMaxSegmentSize)
        => new LeaseTrackingLogSegment(inner.CreateSegment(baseDirectory, baseOffset, createNew, maxSegmentSize), _tracker);
}

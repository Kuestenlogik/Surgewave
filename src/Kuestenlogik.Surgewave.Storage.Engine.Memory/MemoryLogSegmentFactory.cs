using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.Memory;

/// <summary>
/// Factory for creating in-memory log segments without disk I/O.
/// Ideal for testing, ephemeral workloads, and ultra-low-latency scenarios.
/// </summary>
public sealed class MemoryLogSegmentFactory : ILogSegmentFactory
{
    public bool IsPersistent => false;

    public ILogSegment CreateSegment(string baseDirectory, long baseOffset, bool createNew, long maxSegmentSize = ILogSegment.DefaultMaxSegmentSize)
    {
        // Memory segments are always "new" - no persistence to recover from
        return new MemoryLogSegment(baseOffset, maxSegmentSize);
    }
}

/// <summary>
/// Factory methods for creating zero-copy memory-backed log segment factories.
/// </summary>
public static class ZeroCopyMemoryLogSegmentFactory
{
    /// <summary>
    /// Create a zero-copy memory storage factory with pooled buffers.
    /// </summary>
    public static ILogSegmentFactory Create()
    {
        var engineFactory = new MemoryStorageEngineFactory();
        return new StorageEngineSegmentFactory(engineFactory, isPersistent: false);
    }
}

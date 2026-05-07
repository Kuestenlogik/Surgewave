namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Factory interface for creating log segments.
/// Allows dependency injection to configure file-based or memory-based storage.
/// </summary>
public interface ILogSegmentFactory
{
    /// <summary>
    /// Create a new segment or open an existing one.
    /// </summary>
    /// <param name="baseDirectory">Directory path (used for file-based, can be used as identifier for memory-based)</param>
    /// <param name="baseOffset">Base offset for this segment</param>
    /// <param name="createNew">True to create a new segment, false to open existing</param>
    /// <param name="maxSegmentSize">Maximum segment size in bytes</param>
    ILogSegment CreateSegment(string baseDirectory, long baseOffset, bool createNew, long maxSegmentSize = ILogSegment.DefaultMaxSegmentSize);

    /// <summary>
    /// Whether this factory creates persistent (file-based) segments.
    /// </summary>
    bool IsPersistent { get; }
}

/// <summary>
/// Storage backend options for LogManager.
/// </summary>
public enum StorageBackend
{
    /// <summary>
    /// Traditional file-based storage.
    /// Use FileLogSegmentFactory from Kuestenlogik.Surgewave.Storage.FileSystem.
    /// </summary>
    File,

    /// <summary>
    /// Zero-copy file storage with memory-mapped reads.
    /// Better performance for high-throughput workloads.
    /// Use FileLogSegmentFactory.Create(useMmap: true) from Kuestenlogik.Surgewave.Storage.FileSystem.
    /// </summary>
    ZeroCopyWal,

    /// <summary>
    /// Zero-copy in-memory storage with pooled buffers.
    /// Best for testing and ephemeral workloads.
    /// Use ZeroCopyMemoryLogSegmentFactory from Kuestenlogik.Surgewave.Storage.Memory.
    /// </summary>
    ZeroCopyMemory,

    /// <summary>
    /// Basic in-memory storage.
    /// Use MemoryLogSegmentFactory from Kuestenlogik.Surgewave.Storage.Memory.
    /// </summary>
    Memory
}

/// <summary>
/// Factory methods for creating log segment factories.
/// All storage backends now require their respective packages.
/// </summary>
public static class LogSegmentFactories
{
    /// <summary>
    /// Create a log segment factory for the specified storage backend.
    /// Note: All storage backends now require their respective packages:
    /// - File: Use FileLogSegmentFactory.Create() from Kuestenlogik.Surgewave.Storage.FileSystem
    /// - ZeroCopyWal: Use FileLogSegmentFactory.Create(useMmap: true) from Kuestenlogik.Surgewave.Storage.FileSystem
    /// - ZeroCopyMemory: Use ZeroCopyMemoryLogSegmentFactory.Create() from Kuestenlogik.Surgewave.Storage.Memory
    /// - Memory: Use MemoryLogSegmentFactory from Kuestenlogik.Surgewave.Storage.Memory
    /// </summary>
    public static ILogSegmentFactory Create(StorageBackend backend)
    {
        return backend switch
        {
            StorageBackend.File =>
                throw new InvalidOperationException(
                    $"Storage backend {backend} requires Kuestenlogik.Surgewave.Storage.FileSystem package. " +
                    "Use FileLogSegmentFactory.Create() from Kuestenlogik.Surgewave.Storage.FileSystem namespace."),
            StorageBackend.Memory =>
                throw new InvalidOperationException(
                    $"Storage backend {backend} requires Kuestenlogik.Surgewave.Storage.Memory package. " +
                    "Use new MemoryLogSegmentFactory() from Kuestenlogik.Surgewave.Storage.Memory namespace."),
            StorageBackend.ZeroCopyWal =>
                throw new InvalidOperationException(
                    $"Storage backend {backend} requires Kuestenlogik.Surgewave.Storage.FileSystem package. " +
                    "Use FileLogSegmentFactory.Create(useMmap: true) from Kuestenlogik.Surgewave.Storage.FileSystem namespace."),
            StorageBackend.ZeroCopyMemory =>
                throw new InvalidOperationException(
                    $"Storage backend {backend} requires Kuestenlogik.Surgewave.Storage.Memory package. " +
                    "Use ZeroCopyMemoryLogSegmentFactory.Create() from Kuestenlogik.Surgewave.Storage.Memory namespace."),
            _ => throw new ArgumentOutOfRangeException(nameof(backend), backend, "Unknown storage backend")
        };
    }
}

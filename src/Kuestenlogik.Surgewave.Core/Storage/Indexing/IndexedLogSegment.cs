using Microsoft.Win32.SafeHandles;

namespace Kuestenlogik.Surgewave.Core.Storage.Indexing;

/// <summary>
/// Decorator that wraps any ILogSegment with custom indexing support.
/// Intercepts AppendBatchAsync to notify registered custom indexers.
/// </summary>
public sealed class IndexedLogSegment : ILogSegment, IFileLogSegment, IMemoryLogSegment
{
    private readonly ILogSegment _inner;
    private readonly CustomIndexerRegistry _indexers;
    private readonly string _baseDirectory;
    private long _filePosition; // Track file position for custom indexers

    public IndexedLogSegment(ILogSegment inner, CustomIndexerRegistry indexers, string baseDirectory)
    {
        _inner = inner;
        _indexers = indexers;
        _baseDirectory = baseDirectory;
        _filePosition = inner.Size;

        // Load existing custom indexes
        _indexers.LoadAll(baseDirectory, inner.BaseOffset);
    }

    // Delegate all ILogSegment properties to inner
    public long BaseOffset => _inner.BaseOffset;
    public long CurrentOffset => _inner.CurrentOffset;
    public long Size => _inner.Size;
    public bool IsFull => _inner.IsFull;
    public DateTime CreatedAt => _inner.CreatedAt;
    public long MaxTimestamp => _inner.MaxTimestamp;

    // IFileLogSegment implementation
    public string LogFilePath => (_inner as IFileLogSegment)?.LogFilePath ?? string.Empty;
    public SafeFileHandle SafeFileHandle => (_inner as IFileLogSegment)?.SafeFileHandle!;

    // IMemoryLogSegment implementation
    public ReadOnlyMemory<byte> GetMemorySlice(long position, int length)
        => (_inner as IMemoryLogSegment)?.GetMemorySlice(position, length) ?? default;

    public long? GetFirstMessageOffset() => _inner.GetFirstMessageOffset();

    public async ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default)
    {
        var filePositionBefore = _filePosition;

        // Append to inner segment
        var result = await _inner.AppendBatchAsync(recordBatch, cancellationToken);

        // Notify custom indexers
        _indexers.OnBatchAppended(result.baseOffset, filePositionBefore, recordBatch);

        // Update tracked position
        _filePosition += recordBatch.Length;

        return result;
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _inner.FlushAsync(cancellationToken);
        await _indexers.FlushAllAsync(cancellationToken);
        await _indexers.SaveAllAsync(_baseDirectory, _inner.BaseOffset, cancellationToken);
    }

    public ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
        => _inner.ReadBatchesAsync(startOffset, maxBytes, cancellationToken);

    public ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
        => _inner.ReadBatchesContiguousAsync(startOffset, maxBytes, cancellationToken);

    public long? GetFilePositionForOffset(long startOffset)
        => _inner.GetFilePositionForOffset(startOffset);

    public long? FindOffsetByTimestamp(long targetTimestamp)
        => _inner.FindOffsetByTimestamp(targetTimestamp);

    public void DeleteFiles()
    {
        _indexers.DeleteAllFiles(_baseDirectory, _inner.BaseOffset);
        _inner.DeleteFiles();
    }

    public void Dispose()
    {
        // Save custom indexes before disposing
        try
        {
            _indexers.SaveAllAsync(_baseDirectory, _inner.BaseOffset, CancellationToken.None)
                .AsTask().Wait(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore save errors during dispose
        }

        _indexers.Dispose();
        _inner.Dispose();
    }
}

/// <summary>
/// Factory decorator that wraps segments with custom indexing support.
/// </summary>
public sealed class IndexedLogSegmentFactory : ILogSegmentFactory
{
    private readonly ILogSegmentFactory _innerFactory;
    private readonly Func<CustomIndexerRegistry> _registryFactory;

    /// <summary>
    /// Create an indexed factory using the global indexer registry.
    /// </summary>
    public IndexedLogSegmentFactory(ILogSegmentFactory innerFactory)
        : this(innerFactory, GlobalCustomIndexerRegistry.CreateRegistryWithAllIndexers)
    {
    }

    /// <summary>
    /// Create an indexed factory with a custom registry factory.
    /// </summary>
    public IndexedLogSegmentFactory(ILogSegmentFactory innerFactory, Func<CustomIndexerRegistry> registryFactory)
    {
        _innerFactory = innerFactory;
        _registryFactory = registryFactory;
    }

    public bool IsPersistent => _innerFactory.IsPersistent;

    public ILogSegment CreateSegment(string baseDirectory, long baseOffset, bool createNew, long maxSegmentSize = ILogSegment.DefaultMaxSegmentSize)
    {
        var inner = _innerFactory.CreateSegment(baseDirectory, baseOffset, createNew, maxSegmentSize);
        var registry = _registryFactory();

        // If no indexers registered, return unwrapped segment for zero overhead
        if (registry.Indexers.Count == 0)
        {
            registry.Dispose();
            return inner;
        }

        return new IndexedLogSegment(inner, registry, baseDirectory);
    }
}

/// <summary>
/// Extension methods for adding custom indexing to factories.
/// </summary>
public static class IndexedLogSegmentExtensions
{
    /// <summary>
    /// Wrap a factory to add custom indexing support using the global registry.
    /// </summary>
    public static ILogSegmentFactory WithCustomIndexing(this ILogSegmentFactory factory)
    {
        return new IndexedLogSegmentFactory(factory);
    }

    /// <summary>
    /// Wrap a factory to add custom indexing support with specific indexers.
    /// </summary>
    public static ILogSegmentFactory WithCustomIndexing(
        this ILogSegmentFactory factory,
        params ICustomIndexerFactory[] indexerFactories)
    {
        return new IndexedLogSegmentFactory(factory, () =>
        {
            var registry = new CustomIndexerRegistry();
            foreach (var indexerFactory in indexerFactories)
            {
                registry.Register(indexerFactory.Create());
            }
            return registry;
        });
    }
}

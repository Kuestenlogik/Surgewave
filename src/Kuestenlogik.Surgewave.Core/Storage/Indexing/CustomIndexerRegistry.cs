namespace Kuestenlogik.Surgewave.Core.Storage.Indexing;

/// <summary>
/// Registry for managing multiple custom indexers.
/// Thread-safe for concurrent access during reads and writes.
/// </summary>
public sealed class CustomIndexerRegistry : IDisposable
{
    private readonly List<ICustomIndexer> _indexers = [];
    private readonly Dictionary<string, ICustomIndexer> _indexersByName = new(StringComparer.OrdinalIgnoreCase);
    private readonly Lock _lock = new();
    private bool _disposed;

    /// <summary>
    /// Get all registered indexers.
    /// </summary>
    public IReadOnlyList<ICustomIndexer> Indexers
    {
        get
        {
            lock (_lock)
            {
                return _indexers.ToList();
            }
        }
    }

    /// <summary>
    /// Register a custom indexer.
    /// </summary>
    /// <exception cref="InvalidOperationException">If an indexer with the same name is already registered</exception>
    public void Register(ICustomIndexer indexer)
    {
        ArgumentNullException.ThrowIfNull(indexer);

        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_indexersByName.ContainsKey(indexer.Name))
                throw new InvalidOperationException($"Indexer '{indexer.Name}' is already registered");

            _indexers.Add(indexer);
            _indexersByName[indexer.Name] = indexer;
        }
    }

    /// <summary>
    /// Unregister a custom indexer.
    /// </summary>
    /// <returns>True if the indexer was found and removed</returns>
    public bool Unregister(string name)
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_indexersByName.TryGetValue(name, out var indexer))
            {
                _indexers.Remove(indexer);
                _indexersByName.Remove(name);
                return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Get a custom indexer by name.
    /// </summary>
    public ICustomIndexer? GetIndexer(string name)
    {
        lock (_lock)
        {
            return _indexersByName.GetValueOrDefault(name);
        }
    }

    /// <summary>
    /// Get a strongly-typed custom indexer by name.
    /// </summary>
    public T? GetIndexer<T>(string name) where T : class, ICustomIndexer
    {
        return GetIndexer(name) as T;
    }

    /// <summary>
    /// Called after a batch is appended. Notifies all registered indexers.
    /// </summary>
    public void OnBatchAppended(long baseOffset, long filePosition, ReadOnlySpan<byte> recordBatch)
    {
        // Take a snapshot of indexers to avoid holding lock during indexer callbacks
        List<ICustomIndexer> indexers;
        lock (_lock)
        {
            if (_disposed || _indexers.Count == 0)
                return;
            indexers = [.. _indexers];
        }

        foreach (var indexer in indexers)
        {
            try
            {
                indexer.OnBatchAppended(baseOffset, filePosition, recordBatch);
            }
            catch (Exception ex)
            {
                // Log but don't fail the write operation due to indexer errors
                System.Diagnostics.Debug.WriteLine($"Custom indexer '{indexer.Name}' failed: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Flush all indexers.
    /// </summary>
    public async ValueTask FlushAllAsync(CancellationToken cancellationToken = default)
    {
        List<ICustomIndexer> indexers;
        lock (_lock)
        {
            if (_disposed || _indexers.Count == 0)
                return;
            indexers = [.. _indexers];
        }

        foreach (var indexer in indexers)
        {
            await indexer.FlushAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Load all indexers from storage.
    /// </summary>
    public void LoadAll(string indexDirectory, long segmentBaseOffset)
    {
        List<ICustomIndexer> indexers;
        lock (_lock)
        {
            if (_disposed || _indexers.Count == 0)
                return;
            indexers = [.. _indexers];
        }

        foreach (var indexer in indexers)
        {
            try
            {
                indexer.Load(indexDirectory, segmentBaseOffset);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to load indexer '{indexer.Name}': {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Save all indexers to storage.
    /// </summary>
    public async ValueTask SaveAllAsync(string indexDirectory, long segmentBaseOffset, CancellationToken cancellationToken = default)
    {
        List<ICustomIndexer> indexers;
        lock (_lock)
        {
            if (_disposed || _indexers.Count == 0)
                return;
            indexers = [.. _indexers];
        }

        foreach (var indexer in indexers)
        {
            await indexer.SaveAsync(indexDirectory, segmentBaseOffset, cancellationToken);
        }
    }

    /// <summary>
    /// Delete all indexer files for a segment.
    /// </summary>
    public void DeleteAllFiles(string indexDirectory, long segmentBaseOffset)
    {
        List<ICustomIndexer> indexers;
        lock (_lock)
        {
            if (_disposed || _indexers.Count == 0)
                return;
            indexers = [.. _indexers];
        }

        foreach (var indexer in indexers)
        {
            try
            {
                indexer.DeleteFiles(indexDirectory, segmentBaseOffset);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Failed to delete files for indexer '{indexer.Name}': {ex.Message}");
            }
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
                return;
            _disposed = true;

            foreach (var indexer in _indexers)
            {
                indexer.Dispose();
            }

            _indexers.Clear();
            _indexersByName.Clear();
        }
    }
}

/// <summary>
/// Global registry for custom indexer factories.
/// Use this to register indexers that should be created for every new segment.
/// </summary>
public static class GlobalCustomIndexerRegistry
{
    private static readonly List<ICustomIndexerFactory> _factories = [];
    private static readonly Lock _lock = new();

    /// <summary>
    /// Register a factory for creating custom indexers.
    /// All registered factories will be invoked when creating new segments.
    /// </summary>
    public static void RegisterFactory(ICustomIndexerFactory factory)
    {
        lock (_lock)
        {
            _factories.Add(factory);
        }
    }

    /// <summary>
    /// Unregister a factory.
    /// </summary>
    public static bool UnregisterFactory(ICustomIndexerFactory factory)
    {
        lock (_lock)
        {
            return _factories.Remove(factory);
        }
    }

    /// <summary>
    /// Create a registry populated with indexers from all registered factories.
    /// </summary>
    public static CustomIndexerRegistry CreateRegistryWithAllIndexers()
    {
        var registry = new CustomIndexerRegistry();

        lock (_lock)
        {
            foreach (var factory in _factories)
            {
                var indexer = factory.Create();
                registry.Register(indexer);
            }
        }

        return registry;
    }

    /// <summary>
    /// Clear all registered factories.
    /// </summary>
    public static void ClearFactories()
    {
        lock (_lock)
        {
            _factories.Clear();
        }
    }
}

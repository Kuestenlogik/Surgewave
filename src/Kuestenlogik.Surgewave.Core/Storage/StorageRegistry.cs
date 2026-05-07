using System.Collections.Frozen;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Instance-based registry for storage engine factories.
/// Each broker/runtime can have its own registry with different storage backends.
/// </summary>
public sealed class StorageRegistry
{
    private readonly Dictionary<string, Func<ILogSegmentFactory>> _factories = new(StringComparer.OrdinalIgnoreCase);
    private FrozenDictionary<string, Func<ILogSegmentFactory>>? _frozenFactories;
    private readonly Lock _lock = new();

    /// <summary>
    /// Creates a new empty storage registry.
    /// </summary>
    public StorageRegistry()
    {
    }

    /// <summary>
    /// Creates a storage registry with pre-registered factories.
    /// </summary>
    public StorageRegistry(IEnumerable<KeyValuePair<string, Func<ILogSegmentFactory>>> factories)
    {
        foreach (var kvp in factories)
        {
            _factories[kvp.Key] = kvp.Value;
        }
    }

    /// <summary>
    /// Register a storage factory by name.
    /// </summary>
    /// <param name="name">Storage name (case-insensitive), e.g., "memory", "file", "rocksdb"</param>
    /// <param name="factory">Factory function that creates ILogSegmentFactory instances</param>
    /// <returns>This registry for fluent chaining</returns>
    public StorageRegistry Register(string name, Func<ILogSegmentFactory> factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(factory);

        lock (_lock)
        {
            _factories[name] = factory;
            _frozenFactories = null; // Invalidate cache
        }
        return this;
    }

    /// <summary>
    /// Register a storage factory by name using an existing factory instance.
    /// </summary>
    public StorageRegistry Register(string name, ILogSegmentFactory factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        return Register(name, () => factory);
    }

    /// <summary>
    /// Unregister a storage factory.
    /// </summary>
    public bool Unregister(string name)
    {
        lock (_lock)
        {
            var removed = _factories.Remove(name);
            if (removed)
            {
                _frozenFactories = null;
            }
            return removed;
        }
    }

    /// <summary>
    /// Get a storage factory by name.
    /// </summary>
    /// <param name="name">Storage name (case-insensitive)</param>
    /// <returns>The factory, or null if not found</returns>
    public ILogSegmentFactory? Get(string name)
    {
        var factories = GetFrozenFactories();
        return factories.TryGetValue(name, out var factory) ? factory() : null;
    }

    /// <summary>
    /// Get a storage factory by name, throwing if not found.
    /// </summary>
    public ILogSegmentFactory GetRequired(string name)
    {
        return Get(name) ?? throw new KeyNotFoundException(
            $"Storage '{name}' not registered. Available: {string.Join(", ", GetRegisteredNames())}");
    }

    /// <summary>
    /// Check if a storage is registered.
    /// </summary>
    public bool IsRegistered(string name)
    {
        return GetFrozenFactories().ContainsKey(name);
    }

    /// <summary>
    /// Get all registered storage names.
    /// </summary>
    public IEnumerable<string> GetRegisteredNames()
    {
        return GetFrozenFactories().Keys;
    }

    /// <summary>
    /// Clear all registered factories.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _factories.Clear();
            _frozenFactories = null;
        }
    }

    /// <summary>
    /// Create a copy of this registry.
    /// </summary>
    public StorageRegistry Clone()
    {
        lock (_lock)
        {
            return new StorageRegistry(_factories);
        }
    }

    private FrozenDictionary<string, Func<ILogSegmentFactory>> GetFrozenFactories()
    {
        var frozen = _frozenFactories;
        if (frozen != null)
            return frozen;

        lock (_lock)
        {
            _frozenFactories ??= _factories.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
            return _frozenFactories;
        }
    }

    #region Static Default Registry (for convenience in simple scenarios)

    private static readonly Lazy<StorageRegistry> _default = new(() => new StorageRegistry());

    /// <summary>
    /// Default shared registry for simple single-broker scenarios.
    /// For multi-broker setups, create separate StorageRegistry instances.
    /// </summary>
    public static StorageRegistry Default => _default.Value;

    /// <summary>
    /// Create a new registry pre-populated with all storages from the default registry.
    /// Useful for creating isolated registries that start with the standard storages.
    /// </summary>
    public static StorageRegistry CreateFromDefault()
    {
        return Default.Clone();
    }

    #endregion
}

/// <summary>
/// Extension methods for StorageRegistry integration with builders.
/// </summary>
public static class StorageRegistryExtensions
{
    /// <summary>
    /// Try to get a factory from the default registry, returning false if not found.
    /// </summary>
    public static bool TryGetFromDefault(string storageName, out ILogSegmentFactory? factory)
    {
        factory = StorageRegistry.Default.Get(storageName);
        return factory != null;
    }
}

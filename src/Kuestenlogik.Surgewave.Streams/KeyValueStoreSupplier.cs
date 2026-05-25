using Kuestenlogik.Surgewave.Streams.Changelog;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Key-value store supplier.
/// </summary>
public sealed class KeyValueStoreSupplier<TKey, TValue> : IStoreSupplier<IKeyValueStore<TKey, TValue>>
    where TKey : notnull
{
    public string Name { get; }
    private readonly IComparer<TKey>? _comparer;

    /// <summary>
    /// Optional changelog configuration. When set and enabled, the store will be wrapped
    /// with a ChangelogBackedStore for automatic changelog writing.
    /// </summary>
    public ChangelogConfig? ChangelogConfig { get; init; }

    /// <summary>
    /// The application ID, used for changelog topic naming.
    /// </summary>
    public string? ApplicationId { get; init; }

    /// <summary>
    /// Optional key serde for changelog serialization.
    /// </summary>
    public ISerde<TKey>? KeySerde { get; init; }

    /// <summary>
    /// Optional value serde for changelog serialization.
    /// </summary>
    public ISerde<TValue>? ValueSerde { get; init; }

    public KeyValueStoreSupplier(string name, IComparer<TKey>? comparer = null)
    {
        Name = name;
        _comparer = comparer;
    }

    public IKeyValueStore<TKey, TValue> Get()
    {
        var store = new InMemoryKeyValueStore<TKey, TValue>(Name, _comparer);

        if (ChangelogConfig is { Enabled: true } && ApplicationId != null && KeySerde != null && ValueSerde != null)
        {
            return new ChangelogBackedStore<TKey, TValue>(store, KeySerde, ValueSerde, ApplicationId);
        }

        return store;
    }
}

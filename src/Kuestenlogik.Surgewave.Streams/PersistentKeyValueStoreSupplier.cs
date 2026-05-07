namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Persistent store builder for configuring store options.
/// </summary>
public sealed class PersistentKeyValueStoreSupplier<TKey, TValue> : IStoreSupplier<IKeyValueStore<TKey, TValue>>
    where TKey : notnull
{
    public string Name { get; }
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly IComparer<TKey>? _comparer;

    public PersistentKeyValueStoreSupplier(string name, ISerde<TKey> keySerde, ISerde<TValue> valueSerde, IComparer<TKey>? comparer = null)
    {
        Name = name;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _comparer = comparer;
    }

    public IKeyValueStore<TKey, TValue> Get() => new PersistentKeyValueStore<TKey, TValue>(Name, _keySerde, _valueSerde, _comparer);
}

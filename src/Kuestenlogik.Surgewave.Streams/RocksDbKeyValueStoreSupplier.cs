namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Supplier for RocksDB-backed key-value stores.
/// </summary>
public sealed class RocksDbKeyValueStoreSupplier<TKey, TValue> : IStoreSupplier<IKeyValueStore<TKey, TValue>>
    where TKey : notnull
{
    public string Name { get; }
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly IComparer<TKey>? _comparer;
    private readonly RocksDbStoreConfig? _config;

    public RocksDbKeyValueStoreSupplier(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        IComparer<TKey>? comparer = null,
        RocksDbStoreConfig? config = null)
    {
        Name = name;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _comparer = comparer;
        _config = config;
    }

    public IKeyValueStore<TKey, TValue> Get()
        => new RocksDbKeyValueStore<TKey, TValue>(Name, _keySerde, _valueSerde, _comparer, _config);
}

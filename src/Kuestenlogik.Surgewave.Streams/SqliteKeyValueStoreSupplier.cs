namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Supplier for SQLite-backed key-value stores.
/// </summary>
public sealed class SqliteKeyValueStoreSupplier<TKey, TValue> : IStoreSupplier<IKeyValueStore<TKey, TValue>>
    where TKey : notnull
{
    public string Name { get; }
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly SqliteStoreConfig? _config;

    public SqliteKeyValueStoreSupplier(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        SqliteStoreConfig? config = null)
    {
        Name = name;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _config = config;
    }

    public IKeyValueStore<TKey, TValue> Get()
        => new SqliteKeyValueStore<TKey, TValue>(Name, _keySerde, _valueSerde, _config);
}

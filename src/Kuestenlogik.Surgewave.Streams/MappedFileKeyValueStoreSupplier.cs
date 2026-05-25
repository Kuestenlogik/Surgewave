namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Supplier for memory-mapped file key-value stores.
/// </summary>
public sealed class MappedFileKeyValueStoreSupplier<TKey, TValue> : IStoreSupplier<IKeyValueStore<TKey, TValue>>
    where TKey : notnull
{
    public string Name { get; }
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly MappedFileStoreConfig? _config;

    public MappedFileKeyValueStoreSupplier(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        MappedFileStoreConfig? config = null)
    {
        Name = name;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _config = config;
    }

    public IKeyValueStore<TKey, TValue> Get()
        => new MappedFileKeyValueStore<TKey, TValue>(Name, _keySerde, _valueSerde, _config);
}

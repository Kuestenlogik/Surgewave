namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Persistent window store supplier.
/// </summary>
public sealed class PersistentWindowStoreSupplier<TKey, TValue> : IStoreSupplier<IWindowStore<TKey, TValue>>
    where TKey : notnull
{
    public string Name { get; }
    private readonly TimeSpan _windowSize;
    private readonly TimeSpan _retentionPeriod;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;

    public PersistentWindowStoreSupplier(string name, TimeSpan windowSize, TimeSpan retentionPeriod,
        ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
    {
        Name = name;
        _windowSize = windowSize;
        _retentionPeriod = retentionPeriod;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
    }

    public IWindowStore<TKey, TValue> Get() => new PersistentWindowStore<TKey, TValue>(Name, _windowSize, _retentionPeriod, _keySerde, _valueSerde);
}

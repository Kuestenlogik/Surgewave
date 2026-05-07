namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Window store supplier.
/// </summary>
public sealed class WindowStoreSupplier<TKey, TValue> : IStoreSupplier<IWindowStore<TKey, TValue>>
    where TKey : notnull
{
    public string Name { get; }
    private readonly TimeSpan _windowSize;
    private readonly TimeSpan _retentionPeriod;

    public WindowStoreSupplier(string name, TimeSpan windowSize, TimeSpan retentionPeriod)
    {
        Name = name;
        _windowSize = windowSize;
        _retentionPeriod = retentionPeriod;
    }

    public IWindowStore<TKey, TValue> Get() => new InMemoryWindowStore<TKey, TValue>(Name, _windowSize, _retentionPeriod);
}

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Session store supplier.
/// </summary>
public sealed class SessionStoreSupplier<TKey, TValue> : IStoreSupplier<ISessionStore<TKey, TValue>>
    where TKey : notnull
{
    public string Name { get; }
    private readonly TimeSpan _retentionPeriod;

    public SessionStoreSupplier(string name, TimeSpan retentionPeriod)
    {
        Name = name;
        _retentionPeriod = retentionPeriod;
    }

    public ISessionStore<TKey, TValue> Get() => new InMemorySessionStore<TKey, TValue>(Name, _retentionPeriod);
}

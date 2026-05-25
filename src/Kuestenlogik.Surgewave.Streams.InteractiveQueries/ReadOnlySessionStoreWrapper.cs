using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Read-only wrapper for session stores used by Interactive Queries.
/// </summary>
public sealed class ReadOnlySessionStoreWrapper<TKey, TValue>
    where TKey : notnull
{
    private readonly ISessionStore<TKey, TValue> _inner;

    internal ReadOnlySessionStoreWrapper(ISessionStore<TKey, TValue> inner)
    {
        _inner = inner;
    }

    public IEnumerable<KeyValue<Windowed<TKey>, TValue>> FindSessions(
        TKey key, long earliestSessionEndTime, long latestSessionStartTime)
        => _inner.FindSessions(key, earliestSessionEndTime, latestSessionStartTime);

    public TValue? FetchSession(TKey key, long earliestSessionEndTime, long latestSessionStartTime)
        => _inner.FetchSession(key, earliestSessionEndTime, latestSessionStartTime);
}

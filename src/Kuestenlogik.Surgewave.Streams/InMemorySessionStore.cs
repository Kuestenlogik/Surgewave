using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// In-memory session store implementation.
/// </summary>
public sealed class InMemorySessionStore<TKey, TValue> : ISessionStore<TKey, TValue>
    where TKey : notnull
{
    private readonly ConcurrentDictionary<(TKey Key, long Start, long End), TValue> _store = new();
    private readonly TimeSpan _retentionPeriod;
    private ProcessorContext? _context;

    public string Name { get; }
    public bool Persistent => false;

    public InMemorySessionStore(string name, TimeSpan retentionPeriod)
    {
        Name = name;
        _retentionPeriod = retentionPeriod;
    }

    public void Init(ProcessorContext context) => _context = context;

    public void Put(Windowed<TKey> sessionKey, TValue value)
    {
        _store[(sessionKey.Key, sessionKey.Window.StartMs, sessionKey.Window.EndMs)] = value;
        ExpireOldSessions();
    }

    public TValue? FetchSession(TKey key, long earliestSessionEndTime, long latestSessionStartTime)
    {
        var session = _store
            .Where(kv => EqualityComparer<TKey>.Default.Equals(kv.Key.Key, key) &&
                         kv.Key.End >= earliestSessionEndTime &&
                         kv.Key.Start <= latestSessionStartTime)
            .FirstOrDefault();

        return session.Key.Key != null ? session.Value : default;
    }

    public IEnumerable<KeyValue<Windowed<TKey>, TValue>> FindSessions(TKey key, long earliestSessionEndTime, long latestSessionStartTime)
    {
        return _store
            .Where(kv => EqualityComparer<TKey>.Default.Equals(kv.Key.Key, key) &&
                         kv.Key.End >= earliestSessionEndTime &&
                         kv.Key.Start <= latestSessionStartTime)
            .Select(kv => new KeyValue<Windowed<TKey>, TValue>(
                new Windowed<TKey>(kv.Key.Key, new Window(kv.Key.Start, kv.Key.End)),
                kv.Value));
    }

    public void Remove(Windowed<TKey> sessionKey)
    {
        _store.TryRemove((sessionKey.Key, sessionKey.Window.StartMs, sessionKey.Window.EndMs), out _);
    }

    private void ExpireOldSessions()
    {
        var cutoff = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)_retentionPeriod.TotalMilliseconds;
        foreach (var key in _store.Keys)
        {
            if (key.End < cutoff)
            {
                _store.TryRemove(key, out _);
            }
        }
    }

    public void Flush() { }
    public void Close() => _store.Clear();
    public void Dispose() => Close();
}

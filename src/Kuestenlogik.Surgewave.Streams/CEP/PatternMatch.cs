using System.Collections;

namespace Kuestenlogik.Surgewave.Streams.CEP;

/// <summary>
/// Represents a successful pattern match containing the matched events.
/// </summary>
/// <typeparam name="T">The event type</typeparam>
public sealed class PatternMatch<T> : IReadOnlyDictionary<string, IReadOnlyList<T>>
{
    private readonly Dictionary<string, List<T>> _events = new();
    private readonly long _startTimestamp;
    private readonly long _endTimestamp;

    internal PatternMatch(long startTimestamp)
    {
        _startTimestamp = startTimestamp;
        _endTimestamp = startTimestamp;
    }

    internal void Add(string patternName, T @event, long timestamp)
    {
        if (!_events.TryGetValue(patternName, out var list))
        {
            list = new List<T>();
            _events[patternName] = list;
        }
        list.Add(@event);
    }

    /// <summary>
    /// Gets the first event matched for a pattern.
    /// </summary>
    public T? GetFirst(string patternName)
    {
        return _events.TryGetValue(patternName, out var list) && list.Count > 0
            ? list[0]
            : default;
    }

    /// <summary>
    /// Gets the last event matched for a pattern.
    /// </summary>
    public T? GetLast(string patternName)
    {
        return _events.TryGetValue(patternName, out var list) && list.Count > 0
            ? list[^1]
            : default;
    }

    /// <summary>
    /// Gets all events matched for a pattern.
    /// </summary>
    public IReadOnlyList<T> Get(string patternName)
    {
        return _events.TryGetValue(patternName, out var list)
            ? list
            : Array.Empty<T>();
    }

    /// <summary>
    /// Gets all matched events across all patterns in order.
    /// </summary>
    public IReadOnlyList<T> GetAll()
    {
        return _events.Values.SelectMany(x => x).ToList();
    }

    /// <summary>
    /// Gets the pattern names that have matched events.
    /// </summary>
    public IReadOnlyCollection<string> PatternNames => _events.Keys;

    /// <summary>
    /// Gets the timestamp when the first event in the match occurred.
    /// </summary>
    public long StartTimestamp => _startTimestamp;

    /// <summary>
    /// Gets the timestamp when the last event in the match occurred.
    /// </summary>
    public long EndTimestamp => _endTimestamp;

    // IReadOnlyDictionary implementation
    public IReadOnlyList<T> this[string key] => Get(key);
    public IEnumerable<string> Keys => _events.Keys;
    public IEnumerable<IReadOnlyList<T>> Values => _events.Values.Select(l => (IReadOnlyList<T>)l);
    public int Count => _events.Count;
    public bool ContainsKey(string key) => _events.ContainsKey(key);
    public bool TryGetValue(string key, out IReadOnlyList<T> value)
    {
        if (_events.TryGetValue(key, out var list))
        {
            value = list;
            return true;
        }
        value = Array.Empty<T>();
        return false;
    }
    public IEnumerator<KeyValuePair<string, IReadOnlyList<T>>> GetEnumerator()
    {
        return _events.Select(kv => new KeyValuePair<string, IReadOnlyList<T>>(kv.Key, kv.Value)).GetEnumerator();
    }
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

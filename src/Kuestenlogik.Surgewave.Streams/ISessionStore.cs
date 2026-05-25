using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Session store interface for session-windowed aggregations.
/// Sessions are defined by inactivity gaps between records.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface ISessionStore<TKey, TValue> : IStateStore
{
    /// <summary>Puts a value for the specified session key (windowed key with start/end time).</summary>
    /// <param name="sessionKey">The windowed session key.</param>
    /// <param name="value">The value.</param>
    void Put(Windowed<TKey> sessionKey, TValue value);

    /// <summary>Fetches the session value for a key within the given time bounds.</summary>
    /// <param name="key">The key.</param>
    /// <param name="earliestSessionEndTime">The earliest session end time (Unix milliseconds).</param>
    /// <param name="latestSessionStartTime">The latest session start time (Unix milliseconds).</param>
    /// <returns>The session value, or null if not found.</returns>
    TValue? FetchSession(TKey key, long earliestSessionEndTime, long latestSessionStartTime);

    /// <summary>Finds all sessions for a key within the given time bounds.</summary>
    /// <param name="key">The key.</param>
    /// <param name="earliestSessionEndTime">The earliest session end time (Unix milliseconds).</param>
    /// <param name="latestSessionStartTime">The latest session start time (Unix milliseconds).</param>
    /// <returns>An enumerable of session key-value pairs.</returns>
    IEnumerable<KeyValue<Windowed<TKey>, TValue>> FindSessions(TKey key, long earliestSessionEndTime, long latestSessionStartTime);

    /// <summary>Removes a session by its windowed key.</summary>
    /// <param name="sessionKey">The windowed session key to remove.</param>
    void Remove(Windowed<TKey> sessionKey);
}

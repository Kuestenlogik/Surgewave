using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Windowed key-value store interface for time-windowed aggregations.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IWindowStore<TKey, TValue> : IStateStore
{
    /// <summary>Puts a value into the store for the specified key and window start time.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="windowStartMs">The window start time in Unix milliseconds.</param>
    void Put(TKey key, TValue value, long windowStartMs);

    /// <summary>Fetches the value for the specified key and window start time.</summary>
    /// <param name="key">The key.</param>
    /// <param name="windowStartMs">The window start time in Unix milliseconds.</param>
    /// <returns>The value, or null if not found.</returns>
    TValue? Fetch(TKey key, long windowStartMs);

    /// <summary>Fetches all windowed values for the specified key within a time range.</summary>
    /// <param name="key">The key.</param>
    /// <param name="timeFrom">The start of the time range (inclusive, Unix milliseconds).</param>
    /// <param name="timeTo">The end of the time range (inclusive, Unix milliseconds).</param>
    /// <returns>An enumerable of windowed key-value pairs.</returns>
    IEnumerable<KeyValue<Windowed<TKey>, TValue>> Fetch(TKey key, long timeFrom, long timeTo);

    /// <summary>Fetches all windowed values across all keys within a time range.</summary>
    /// <param name="timeFrom">The start of the time range (inclusive, Unix milliseconds).</param>
    /// <param name="timeTo">The end of the time range (inclusive, Unix milliseconds).</param>
    /// <returns>An enumerable of windowed key-value pairs.</returns>
    IEnumerable<KeyValue<Windowed<TKey>, TValue>> FetchAll(long timeFrom, long timeTo);
}

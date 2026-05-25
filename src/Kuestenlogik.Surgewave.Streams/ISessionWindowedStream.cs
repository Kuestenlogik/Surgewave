using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// A session-windowed stream that supports session-based aggregation operations.
/// Sessions are defined by inactivity gaps; records within the gap are merged into the same session.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface ISessionWindowedStream<TKey, TValue>
{
    /// <summary>Counts the number of records per session-windowed key.</summary>
    /// <returns>A table mapping each session-windowed key to its count.</returns>
    ITable<Windowed<TKey>, long> Count();

    /// <summary>Combines values with the same session-windowed key using a reducer function.</summary>
    /// <param name="reducer">A function that combines two values into one.</param>
    /// <returns>A table mapping each session-windowed key to the reduced value.</returns>
    ITable<Windowed<TKey>, TValue> Reduce(Func<TValue, TValue, TValue> reducer);

    /// <summary>Aggregates values per session-windowed key with session merging support.</summary>
    /// <typeparam name="TAggregate">The aggregate result type.</typeparam>
    /// <param name="initializer">A function that provides the initial aggregate value.</param>
    /// <param name="aggregator">A function that combines a new value with the current aggregate.</param>
    /// <param name="merger">A function that merges two session aggregates when sessions are combined.</param>
    /// <returns>A table mapping each session-windowed key to its aggregate.</returns>
    ITable<Windowed<TKey>, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator,
        Func<TKey, TAggregate, TAggregate, TAggregate> merger);
}

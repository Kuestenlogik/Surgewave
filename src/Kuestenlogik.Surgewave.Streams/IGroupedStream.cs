using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// A grouped stream that supports aggregation operations such as count, reduce, and aggregate.
/// Created by calling <see cref="IStream{TKey,TValue}.GroupByKey"/> or <see cref="IStream{TKey,TValue}.GroupBy{TNewKey}"/>.
/// </summary>
/// <typeparam name="TKey">The grouping key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IGroupedStream<TKey, TValue>
{
    /// <summary>Counts the number of records per key.</summary>
    /// <returns>A table mapping each key to its record count.</returns>
    ITable<TKey, long> Count();

    /// <summary>Combines values with the same key using a reducer function.</summary>
    /// <param name="reducer">A function that combines two values into one.</param>
    /// <returns>A table mapping each key to the reduced value.</returns>
    ITable<TKey, TValue> Reduce(Func<TValue, TValue, TValue> reducer);

    /// <summary>Aggregates values per key using an initializer and aggregator function.</summary>
    /// <typeparam name="TAggregate">The aggregate result type.</typeparam>
    /// <param name="initializer">A function that provides the initial aggregate value.</param>
    /// <param name="aggregator">A function that combines a new value with the current aggregate.</param>
    /// <returns>A table mapping each key to its aggregate.</returns>
    ITable<TKey, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator);

    /// <summary>Windows the grouped stream using tumbling (fixed-size, non-overlapping) windows.</summary>
    /// <param name="windows">The tumbling window specification.</param>
    /// <returns>A time-windowed stream for windowed aggregations.</returns>
    ITimeWindowedStream<TKey, TValue> WindowedBy(TumblingWindows windows);

    /// <summary>Windows the grouped stream using hopping (fixed-size, overlapping) windows.</summary>
    /// <param name="windows">The hopping window specification.</param>
    /// <returns>A time-windowed stream for windowed aggregations.</returns>
    ITimeWindowedStream<TKey, TValue> WindowedBy(HoppingWindows windows);

    /// <summary>Windows the grouped stream using sliding windows.</summary>
    /// <param name="windows">The sliding window specification.</param>
    /// <returns>A time-windowed stream for windowed aggregations.</returns>
    ITimeWindowedStream<TKey, TValue> WindowedBy(SlidingWindows windows);

    /// <summary>Windows the grouped stream using session windows (gap-based).</summary>
    /// <param name="windows">The session window specification.</param>
    /// <returns>A session-windowed stream for windowed aggregations.</returns>
    ISessionWindowedStream<TKey, TValue> WindowedBy(SessionWindows windows);
}

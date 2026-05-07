using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// A time-windowed stream that supports windowed aggregation operations.
/// Created by calling <see cref="IGroupedStream{TKey,TValue}.WindowedBy(TumblingWindows)"/> or similar.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface ITimeWindowedStream<TKey, TValue>
{
    /// <summary>Counts the number of records per windowed key.</summary>
    /// <returns>A table mapping each windowed key to its count.</returns>
    ITable<Windowed<TKey>, long> Count();

    /// <summary>Combines values with the same windowed key using a reducer function.</summary>
    /// <param name="reducer">A function that combines two values into one.</param>
    /// <returns>A table mapping each windowed key to the reduced value.</returns>
    ITable<Windowed<TKey>, TValue> Reduce(Func<TValue, TValue, TValue> reducer);

    /// <summary>Aggregates values per windowed key.</summary>
    /// <typeparam name="TAggregate">The aggregate result type.</typeparam>
    /// <param name="initializer">A function that provides the initial aggregate value.</param>
    /// <param name="aggregator">A function that combines a new value with the current aggregate.</param>
    /// <returns>A table mapping each windowed key to its aggregate.</returns>
    ITable<Windowed<TKey>, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator);

    /// <summary>Aggregates values per windowed key with materialization to a named state store.</summary>
    /// <typeparam name="TAggregate">The aggregate result type.</typeparam>
    /// <param name="initializer">A function that provides the initial aggregate value.</param>
    /// <param name="aggregator">A function that combines a new value with the current aggregate.</param>
    /// <param name="materialized">The materialization configuration.</param>
    /// <returns>A table mapping each windowed key to its aggregate.</returns>
    ITable<Windowed<TKey>, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator,
        Materialized<TKey, TAggregate> materialized);
}

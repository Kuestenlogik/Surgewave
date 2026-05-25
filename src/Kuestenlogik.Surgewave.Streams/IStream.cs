using Kuestenlogik.Surgewave.Streams.Processors;
using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// An abstraction of a record stream of key-value pairs.
/// Provides stateless transformations, stateful operations, joins, and terminal operations
/// for building stream processing topologies.
/// </summary>
/// <typeparam name="TKey">The key type of records in this stream.</typeparam>
/// <typeparam name="TValue">The value type of records in this stream.</typeparam>
/// <example>
/// <code>
/// var builder = new StreamsBuilder();
/// builder.Stream&lt;string, Order&gt;("orders")
///     .Filter((key, order) => order.Amount > 100)
///     .MapValues(order => new OrderSummary { Total = order.Amount })
///     .To("high-value-orders");
/// </code>
/// </example>
public interface IStream<TKey, TValue>
{
    /// <summary>Filters records, keeping only those matching the predicate.</summary>
    /// <param name="predicate">A function that returns true for records to keep.</param>
    /// <returns>A filtered stream.</returns>
    IStream<TKey, TValue> Filter(Func<TKey, TValue, bool> predicate);

    /// <summary>Filters records, removing those matching the predicate (inverse of <see cref="Filter"/>).</summary>
    /// <param name="predicate">A function that returns true for records to remove.</param>
    /// <returns>A filtered stream.</returns>
    IStream<TKey, TValue> FilterNot(Func<TKey, TValue, bool> predicate);

    /// <summary>Transforms the value of each record, keeping the key unchanged.</summary>
    /// <typeparam name="TNewValue">The new value type.</typeparam>
    /// <param name="mapper">A function to transform the value.</param>
    /// <returns>A stream with transformed values.</returns>
    IStream<TKey, TNewValue> MapValues<TNewValue>(Func<TValue, TNewValue> mapper);

    /// <summary>Transforms the value of each record using both key and value, keeping the key unchanged.</summary>
    /// <typeparam name="TNewValue">The new value type.</typeparam>
    /// <param name="mapper">A function that takes key and value and returns a new value.</param>
    /// <returns>A stream with transformed values.</returns>
    IStream<TKey, TNewValue> MapValues<TNewValue>(Func<TKey, TValue, TNewValue> mapper);

    /// <summary>Transforms both key and value of each record.</summary>
    /// <typeparam name="TNewKey">The new key type.</typeparam>
    /// <typeparam name="TNewValue">The new value type.</typeparam>
    /// <param name="mapper">A function that maps each record to a new key-value pair.</param>
    /// <returns>A stream with new key and value types.</returns>
    IStream<TNewKey, TNewValue> Map<TNewKey, TNewValue>(Func<TKey, TValue, KeyValue<TNewKey, TNewValue>> mapper);

    /// <summary>Re-keys each record using a key selector function.</summary>
    /// <typeparam name="TNewKey">The new key type.</typeparam>
    /// <param name="mapper">A function to select a new key from the current key and value.</param>
    /// <returns>A stream with new keys.</returns>
    IStream<TNewKey, TValue> SelectKey<TNewKey>(Func<TKey, TValue, TNewKey> mapper);

    /// <summary>Maps each value to zero or more output values, expanding the stream.</summary>
    /// <typeparam name="TNewValue">The new value type.</typeparam>
    /// <param name="mapper">A function that returns multiple values for each input value.</param>
    /// <returns>A stream with potentially more records than the input.</returns>
    IStream<TKey, TNewValue> FlatMapValues<TNewValue>(Func<TValue, IEnumerable<TNewValue>> mapper);

    /// <summary>Maps each record to zero or more output key-value pairs, expanding the stream.</summary>
    /// <typeparam name="TNewKey">The new key type.</typeparam>
    /// <typeparam name="TNewValue">The new value type.</typeparam>
    /// <param name="mapper">A function that returns multiple key-value pairs for each input record.</param>
    /// <returns>A stream with potentially more records and new types.</returns>
    IStream<TNewKey, TNewValue> FlatMap<TNewKey, TNewValue>(Func<TKey, TValue, IEnumerable<KeyValue<TNewKey, TNewValue>>> mapper);

    /// <summary>Transforms values with access to state stores.</summary>
    /// <typeparam name="TNewValue">The new value type.</typeparam>
    /// <param name="transformer">The stateful value transformer.</param>
    /// <param name="stateStoreNames">Names of state stores the transformer accesses.</param>
    /// <returns>A stream with transformed values.</returns>
    IStream<TKey, TNewValue> TransformValues<TNewValue>(
        IValueTransformerWithKey<TKey, TValue, TNewValue> transformer,
        params string[] stateStoreNames);

    /// <summary>Performs a side-effect action on each record without modifying the stream.</summary>
    /// <param name="action">The action to invoke for each record.</param>
    /// <returns>The same stream, unchanged.</returns>
    IStream<TKey, TValue> Peek(Action<TKey, TValue> action);

    /// <summary>Splits the stream into multiple branches based on predicates.</summary>
    /// <param name="predicates">One predicate per branch; a record goes to the first matching branch.</param>
    /// <returns>An array of streams, one per predicate.</returns>
    IStream<TKey, TValue>[] Branch(params Func<TKey, TValue, bool>[] predicates);

    /// <summary>Merges this stream with another stream of the same types.</summary>
    /// <param name="other">The other stream to merge with.</param>
    /// <returns>A stream containing records from both input streams.</returns>
    IStream<TKey, TValue> Merge(IStream<TKey, TValue> other);

    /// <summary>Repartitions the stream using the current key for partitioning.</summary>
    /// <returns>A repartitioned stream.</returns>
    IStream<TKey, TValue> Repartition();

    /// <summary>Repartitions the stream using a custom partitioner that produces a new key.</summary>
    /// <typeparam name="TNewKey">The new key type.</typeparam>
    /// <param name="partitioner">A function to compute the new key for partitioning.</param>
    /// <returns>A repartitioned stream with the new key type.</returns>
    IStream<TNewKey, TValue> Repartition<TNewKey>(Func<TKey, TValue, TNewKey> partitioner);

    /// <summary>Groups the stream by its existing key for aggregation operations.</summary>
    /// <returns>A grouped stream that supports <c>Count</c>, <c>Reduce</c>, and <c>Aggregate</c>.</returns>
    IGroupedStream<TKey, TValue> GroupByKey();

    /// <summary>Groups the stream by a new key derived from each record.</summary>
    /// <typeparam name="TNewKey">The new grouping key type.</typeparam>
    /// <param name="keySelector">A function to select the grouping key.</param>
    /// <returns>A grouped stream keyed by the selected key.</returns>
    IGroupedStream<TNewKey, TValue> GroupBy<TNewKey>(Func<TKey, TValue, TNewKey> keySelector);

    /// <summary>Materializes the stream into a table (changelog semantics).</summary>
    /// <returns>A table backed by a state store.</returns>
    ITable<TKey, TValue> ToTable();

    /// <summary>Performs an inner join between this stream and another stream within a time window.</summary>
    /// <typeparam name="TOther">The value type of the other stream.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="other">The other stream to join with.</param>
    /// <param name="joiner">A function that combines matching values from both streams.</param>
    /// <param name="windows">The join window defining the time range for matching records.</param>
    /// <returns>A stream of joined results.</returns>
    IStream<TKey, TResult> Join<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue, TOther, TResult> joiner,
        JoinWindows windows);

    /// <summary>Performs a left join between this stream and another stream within a time window.</summary>
    /// <typeparam name="TOther">The value type of the other stream.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="other">The other stream to join with.</param>
    /// <param name="joiner">A function that combines values; the right value may be null.</param>
    /// <param name="windows">The join window defining the time range for matching records.</param>
    /// <returns>A stream of joined results.</returns>
    IStream<TKey, TResult> LeftJoin<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue, TOther?, TResult> joiner,
        JoinWindows windows);

    /// <summary>Performs an outer join between this stream and another stream within a time window.</summary>
    /// <typeparam name="TOther">The value type of the other stream.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="other">The other stream to join with.</param>
    /// <param name="joiner">A function that combines values; either value may be null.</param>
    /// <param name="windows">The join window defining the time range for matching records.</param>
    /// <returns>A stream of joined results.</returns>
    IStream<TKey, TResult> OuterJoin<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue?, TOther?, TResult> joiner,
        JoinWindows windows);

    /// <summary>Performs an inner join between this stream and a table.</summary>
    /// <typeparam name="TTableValue">The value type of the table.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="table">The table to join with.</param>
    /// <param name="joiner">A function that combines stream and table values.</param>
    /// <returns>A stream of joined results.</returns>
    IStream<TKey, TResult> Join<TTableValue, TResult>(
        ITable<TKey, TTableValue> table,
        Func<TValue, TTableValue, TResult> joiner);

    /// <summary>Performs a left join between this stream and a table.</summary>
    /// <typeparam name="TTableValue">The value type of the table.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="table">The table to join with.</param>
    /// <param name="joiner">A function that combines values; the table value may be null.</param>
    /// <returns>A stream of joined results.</returns>
    IStream<TKey, TResult> LeftJoin<TTableValue, TResult>(
        ITable<TKey, TTableValue> table,
        Func<TValue, TTableValue?, TResult> joiner);

    /// <summary>Performs an inner join between this stream and a global table using a key selector.</summary>
    /// <typeparam name="TGlobalKey">The key type of the global table.</typeparam>
    /// <typeparam name="TGlobalValue">The value type of the global table.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="globalTable">The global table to join with.</param>
    /// <param name="keySelector">A function to extract the lookup key for the global table.</param>
    /// <param name="joiner">A function that combines stream and table values.</param>
    /// <returns>A stream of joined results.</returns>
    IStream<TKey, TResult> Join<TGlobalKey, TGlobalValue, TResult>(
        IGlobalTable<TGlobalKey, TGlobalValue> globalTable,
        Func<TKey, TValue, TGlobalKey> keySelector,
        Func<TValue, TGlobalValue, TResult> joiner);

    /// <summary>Adds retry logic to downstream processing with exponential backoff.</summary>
    /// <param name="maxRetries">Maximum number of retry attempts (default: 3).</param>
    /// <returns>A stream with retry semantics.</returns>
    IStream<TKey, TValue> WithRetry(int maxRetries = 3);

    /// <summary>Applies rate limiting to the stream, throttling throughput to the specified rate.</summary>
    /// <param name="recordsPerSecond">Maximum records per second.</param>
    /// <returns>A rate-limited stream.</returns>
    IStream<TKey, TValue> RateLimit(int recordsPerSecond);

    /// <summary>Enables parallel processing of records using multiple threads.</summary>
    /// <param name="degreeOfParallelism">Number of parallel processing threads.</param>
    /// <returns>A stream with parallel processing enabled.</returns>
    IStream<TKey, TValue> Parallel(int degreeOfParallelism);

    /// <summary>Writes the stream to a topic (terminal operation).</summary>
    /// <param name="topic">The output topic name.</param>
    void To(string topic);

    /// <summary>Writes the stream to dynamically selected topics (terminal operation).</summary>
    /// <param name="topicExtractor">A function that determines the target topic for each record.</param>
    void To(Func<TKey, TValue, string> topicExtractor);

    /// <summary>Performs a terminal action on each record in the stream.</summary>
    /// <param name="action">The action to invoke for each record.</param>
    void ForEach(Action<TKey, TValue> action);

    /// <summary>Prints each record to the console (terminal operation for debugging).</summary>
    void Print();
}

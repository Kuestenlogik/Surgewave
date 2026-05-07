// ReSharper disable InconsistentNaming
using Kuestenlogik.Surgewave.Streams;

namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Streams;

/// <summary>
/// Kafka Streams API compatibility aliases.
/// These allow existing Kafka Streams code to compile with minimal changes.
/// Import this namespace to use Kafka-style type names (IKStream, IKTable, etc.).
/// </summary>

// Stream type aliases
public interface IKStream<TKey, TValue> : IStream<TKey, TValue>;
public interface IKTable<TKey, TValue> : ITable<TKey, TValue>;
public interface IKGroupedStream<TKey, TValue> : IGroupedStream<TKey, TValue>;
public interface IKGroupedTable<TKey, TValue> : IGroupedTable<TKey, TValue>;
public interface IGlobalKTable<TKey, TValue> : IGlobalTable<TKey, TValue>;
public interface ITimeWindowedKStream<TKey, TValue> : ITimeWindowedStream<TKey, TValue>;
public interface ISessionWindowedKStream<TKey, TValue> : ISessionWindowedStream<TKey, TValue>;

/// <summary>
/// Extension methods to cast Surgewave types to Kafka-compatible types.
/// </summary>
public static class KafkaStreamExtensions
{
    /// <summary>
    /// Casts a Surgewave IStream to a Kafka-compatible IKStream.
    /// Note: This only works if the actual implementation supports both interfaces.
    /// For new code, prefer using the native Surgewave IStream interface.
    /// </summary>
    public static IKStream<TKey, TValue>? AsKStream<TKey, TValue>(this IStream<TKey, TValue> stream)
        => stream as IKStream<TKey, TValue>;

    /// <summary>
    /// Casts a Surgewave ITable to a Kafka-compatible IKTable.
    /// </summary>
    public static IKTable<TKey, TValue>? AsKTable<TKey, TValue>(this ITable<TKey, TValue> table)
        => table as IKTable<TKey, TValue>;

    /// <summary>
    /// Casts a Surgewave IGroupedStream to a Kafka-compatible IKGroupedStream.
    /// </summary>
    public static IKGroupedStream<TKey, TValue>? AsKGroupedStream<TKey, TValue>(this IGroupedStream<TKey, TValue> stream)
        => stream as IKGroupedStream<TKey, TValue>;

    /// <summary>
    /// Casts a Surgewave IGlobalTable to a Kafka-compatible IGlobalKTable.
    /// </summary>
    public static IGlobalKTable<TKey, TValue>? AsGlobalKTable<TKey, TValue>(this IGlobalTable<TKey, TValue> table)
        => table as IGlobalKTable<TKey, TValue>;
}

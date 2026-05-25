namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Serialization and production configuration for sink topics.
/// Specifies the serdes and optional partitioner for writing records.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class Produced<TKey, TValue>
{
    /// <summary>Gets the key serde.</summary>
    public ISerde<TKey> KeySerde { get; }

    /// <summary>Gets the value serde.</summary>
    public ISerde<TValue> ValueSerde { get; }

    /// <summary>Gets the custom partitioner function, or null for default partitioning. Parameters are (key, value, numPartitions) returning the partition index.</summary>
    public Func<TKey, TValue, int, int>? Partitioner { get; private init; }

    /// <summary>Creates a new Produced instance with the specified serdes.</summary>
    /// <param name="keySerde">The key serde.</param>
    /// <param name="valueSerde">The value serde.</param>
    public Produced(ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
    {
        KeySerde = keySerde;
        ValueSerde = valueSerde;
    }

    /// <summary>Creates a new Produced instance with the specified serdes.</summary>
    /// <param name="keySerde">The key serde.</param>
    /// <param name="valueSerde">The value serde.</param>
    /// <returns>A new Produced instance.</returns>
    public static Produced<TKey, TValue> With(ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
        => new(keySerde, valueSerde);

    /// <summary>Returns a new Produced instance with the specified custom partitioner.</summary>
    /// <param name="partitioner">A function (key, value, numPartitions) that returns the partition index.</param>
    /// <returns>A new Produced instance with the specified partitioner.</returns>
    public Produced<TKey, TValue> WithPartitioner(Func<TKey, TValue, int, int> partitioner)
        => new(KeySerde, ValueSerde) { Partitioner = partitioner };
}

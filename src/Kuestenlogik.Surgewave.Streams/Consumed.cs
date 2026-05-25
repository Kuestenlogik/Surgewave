namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Serialization and consumption configuration for source topics.
/// Specifies the serdes, timestamp extractor, and offset reset policy for a stream or table source.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class Consumed<TKey, TValue>
{
    /// <summary>Gets the key serde.</summary>
    public ISerde<TKey> KeySerde { get; }

    /// <summary>Gets the value serde.</summary>
    public ISerde<TValue> ValueSerde { get; }

    /// <summary>Gets the timestamp extractor, or null to use the default.</summary>
    public ITimestampExtractor? TimestampExtractor { get; private init; }

    /// <summary>Gets the offset reset policy ("earliest" or "latest"), or null to use the default.</summary>
    public string? ResetPolicy { get; private init; }

    /// <summary>Creates a new Consumed instance with the specified serdes.</summary>
    /// <param name="keySerde">The key serde.</param>
    /// <param name="valueSerde">The value serde.</param>
    public Consumed(ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
    {
        KeySerde = keySerde;
        ValueSerde = valueSerde;
    }

    /// <summary>Creates a new Consumed instance with the specified serdes.</summary>
    /// <param name="keySerde">The key serde.</param>
    /// <param name="valueSerde">The value serde.</param>
    /// <returns>A new Consumed instance.</returns>
    public static Consumed<TKey, TValue> With(ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
        => new(keySerde, valueSerde);

    /// <summary>Returns a new Consumed instance with the specified timestamp extractor.</summary>
    /// <param name="extractor">The timestamp extractor to use.</param>
    /// <returns>A new Consumed instance with the specified extractor.</returns>
    public Consumed<TKey, TValue> WithTimestampExtractor(ITimestampExtractor extractor)
        => new(KeySerde, ValueSerde)
        {
            TimestampExtractor = extractor,
            ResetPolicy = ResetPolicy
        };

    /// <summary>Returns a new Consumed instance with the specified offset reset policy.</summary>
    /// <param name="policy">The offset reset policy ("earliest" or "latest").</param>
    /// <returns>A new Consumed instance with the specified reset policy.</returns>
    public Consumed<TKey, TValue> WithOffsetResetPolicy(string policy)
        => new(KeySerde, ValueSerde)
        {
            TimestampExtractor = TimestampExtractor,
            ResetPolicy = policy
        };
}

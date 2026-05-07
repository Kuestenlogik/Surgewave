namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Represents a record to be produced to a topic.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed record ProducerRecord<TKey, TValue>
{
    /// <summary>
    /// The topic to produce to.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partition to produce to. Null means partition will be selected automatically.
    /// </summary>
    public int? Partition { get; init; }

    /// <summary>
    /// The message key.
    /// </summary>
    public TKey? Key { get; init; }

    /// <summary>
    /// The message value.
    /// </summary>
    public required TValue Value { get; init; }

    /// <summary>
    /// Optional timestamp. If not set, the current time will be used.
    /// </summary>
    public DateTimeOffset? Timestamp { get; init; }
}

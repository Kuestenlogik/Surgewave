namespace Confluent.Kafka;

/// <summary>
/// A Kafka message with key and value.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public class Message<TKey, TValue>
{
    /// <summary>
    /// The message key (may be null).
    /// </summary>
    public TKey? Key { get; set; }

    /// <summary>
    /// The message value.
    /// </summary>
    public TValue? Value { get; set; }

    /// <summary>
    /// The message timestamp. If not set, the broker will assign one.
    /// </summary>
    public Timestamp Timestamp { get; set; } = Timestamp.Default;

    /// <summary>
    /// Message headers. May be null.
    /// </summary>
    public Headers? Headers { get; set; }
}

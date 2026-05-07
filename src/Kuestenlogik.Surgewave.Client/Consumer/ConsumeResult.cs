namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Result of consuming a message.
/// </summary>
public sealed record ConsumeResult<TKey, TValue>
{
    /// <summary>
    /// The topic the message came from.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partition the message came from.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// The offset of the message.
    /// </summary>
    public required long Offset { get; init; }

    /// <summary>
    /// The timestamp of the message.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// The deserialized key (may be null).
    /// </summary>
    public TKey? Key { get; init; }

    /// <summary>
    /// The deserialized value.
    /// </summary>
    public required TValue Value { get; init; }

    /// <summary>
    /// Message headers. May be null if no headers were set or if the protocol doesn't support headers.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]>? Headers { get; init; }
}

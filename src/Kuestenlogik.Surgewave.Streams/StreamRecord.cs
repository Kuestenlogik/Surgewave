namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// A record flowing through a stream processing topology, with key, value, timestamp, and metadata.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed record StreamRecord<TKey, TValue>
{
    /// <summary>Gets the record key.</summary>
    public required TKey Key { get; init; }

    /// <summary>Gets the record value.</summary>
    public required TValue Value { get; init; }

    /// <summary>Gets the record timestamp in Unix milliseconds.</summary>
    public long Timestamp { get; init; }

    /// <summary>Gets the source topic, or null if not from a topic.</summary>
    public string? Topic { get; init; }

    /// <summary>Gets the source partition.</summary>
    public int Partition { get; init; }

    /// <summary>Gets the source offset.</summary>
    public long Offset { get; init; }

    /// <summary>Gets the record headers, or null if no headers are present.</summary>
    public IReadOnlyDictionary<string, byte[]>? Headers { get; init; }
}

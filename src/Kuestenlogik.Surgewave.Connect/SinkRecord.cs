namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A record consumed from Kafka/Surgewave for a sink connector.
/// </summary>
public sealed record SinkRecord
{
    /// <summary>
    /// The topic the record was consumed from.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partition the record was consumed from.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// The offset of the record.
    /// </summary>
    public required long Offset { get; init; }

    /// <summary>
    /// The record key (can be null).
    /// </summary>
    public byte[]? Key { get; init; }

    /// <summary>
    /// The record value.
    /// </summary>
    public required byte[] Value { get; init; }

    /// <summary>
    /// The record timestamp.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Headers from the record.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]>? Headers { get; init; }
}

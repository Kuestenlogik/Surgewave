namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Metadata about a record that was produced to Kafka/Surgewave.
/// </summary>
public sealed record RecordMetadata
{
    /// <summary>
    /// The topic the record was written to.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partition the record was written to.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// The offset of the record.
    /// </summary>
    public required long Offset { get; init; }

    /// <summary>
    /// The timestamp of the record.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }
}

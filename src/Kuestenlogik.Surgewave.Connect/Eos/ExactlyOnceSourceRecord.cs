namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// A source record with explicit partition and offset for exactly-once delivery.
/// The source offset is committed atomically with the produced message, ensuring
/// that on connector restart, no duplicates or losses occur.
/// </summary>
public sealed record ExactlyOnceSourceRecord
{
    /// <summary>
    /// Source partition identifier (e.g., database table name, file path, API endpoint).
    /// Used as the key for offset tracking.
    /// </summary>
    public required string SourcePartition { get; init; }

    /// <summary>
    /// Source offset for this record (e.g., database LSN, file position, API cursor).
    /// This gets committed atomically with the produced message.
    /// </summary>
    public required Dictionary<string, string> SourceOffset { get; init; }

    /// <summary>
    /// The destination topic.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The destination partition (null for automatic partitioning).
    /// </summary>
    public int? Partition { get; init; }

    /// <summary>
    /// The record key (can be null).
    /// </summary>
    public byte[]? Key { get; init; }

    /// <summary>
    /// The record value.
    /// </summary>
    public required byte[] Value { get; init; }

    /// <summary>
    /// The record timestamp (null for current time).
    /// </summary>
    public DateTimeOffset? Timestamp { get; init; }

    /// <summary>
    /// Headers for the record.
    /// </summary>
    public IDictionary<string, byte[]>? Headers { get; init; }
}

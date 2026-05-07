namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A record produced by a source connector.
/// </summary>
public sealed record SourceRecord
{
    /// <summary>
    /// The source partition this record belongs to (for offset tracking).
    /// </summary>
    public required IDictionary<string, object> SourcePartition { get; init; }

    /// <summary>
    /// The source offset of this record (for offset tracking).
    /// </summary>
    public required IDictionary<string, object> SourceOffset { get; init; }

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

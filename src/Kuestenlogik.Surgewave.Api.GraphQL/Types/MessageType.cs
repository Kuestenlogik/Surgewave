namespace Kuestenlogik.Surgewave.Api.GraphQL.Types;

/// <summary>
/// Represents a message in the GraphQL schema.
/// </summary>
public sealed class MessageType
{
    /// <summary>
    /// Topic the message belongs to.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Partition within the topic.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Offset of the message within the partition.
    /// </summary>
    public long Offset { get; init; }

    /// <summary>
    /// Timestamp when the message was produced.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Message key (optional, may be null).
    /// </summary>
    public string? Key { get; init; }

    /// <summary>
    /// Message value as a UTF-8 string.
    /// </summary>
    public string? Value { get; init; }

    /// <summary>
    /// Message headers as key-value pairs.
    /// </summary>
    public Dictionary<string, string>? Headers { get; init; }
}

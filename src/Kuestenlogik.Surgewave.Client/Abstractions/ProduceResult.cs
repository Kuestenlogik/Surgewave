namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Result of a produce operation - unified across protocols.
/// </summary>
public sealed record ProduceResult
{
    /// <summary>
    /// The topic the message was produced to.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partition the message was produced to.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// The offset of the produced message.
    /// </summary>
    public required long Offset { get; init; }

    /// <summary>
    /// The timestamp of the produced message.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }
}

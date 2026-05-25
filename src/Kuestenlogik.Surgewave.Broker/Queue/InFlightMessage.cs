using Kuestenlogik.Surgewave.Core.Queue;

namespace Kuestenlogik.Surgewave.Broker.Queue;

/// <summary>
/// Represents a message that has been delivered to a consumer but not yet acknowledged.
/// The message remains in the Surgewave log; this record tracks the invisible-window state only.
/// </summary>
public sealed class InFlightMessage : IInFlightMessage
{
    /// <summary>
    /// Unique identifier for the in-flight slot — formed as "{topic}-{partition}-{offset}".
    /// </summary>
    public required string MessageId { get; init; }

    /// <summary>
    /// Topic the message belongs to.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Partition index within the topic.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// Log offset of the message.
    /// </summary>
    public required long Offset { get; init; }

    /// <summary>
    /// Raw bytes of the record batch containing this message.
    /// </summary>
    public required byte[] Body { get; init; }

    /// <summary>
    /// Number of times this message has been delivered (starts at 1).
    /// </summary>
    public int DeliveryCount { get; set; } = 1;

    /// <summary>
    /// Wall-clock time at which visibility timeout expires.
    /// After this instant the message becomes eligible for re-delivery.
    /// </summary>
    public DateTimeOffset ExpiresAt { get; set; }

    /// <summary>
    /// Identifier of the consumer that currently holds this message, if known.
    /// </summary>
    public string? ConsumerId { get; set; }
}

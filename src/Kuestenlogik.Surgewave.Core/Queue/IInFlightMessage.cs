namespace Kuestenlogik.Surgewave.Core.Queue;

/// <summary>
/// Represents a message that has been delivered to a consumer but not yet acknowledged.
/// The message remains in the Surgewave log; this record tracks the invisible-window state only.
/// </summary>
public interface IInFlightMessage
{
    /// <summary>
    /// Unique identifier for the in-flight slot.
    /// Formed as "{topic}-{partition}-{offset}" by the concrete QueueView implementation.
    /// Used as the key for <see cref="IQueueView.Ack"/>, <see cref="IQueueView.Nack(string, bool)"/>,
    /// and <see cref="IQueueView.RejectAsync"/>.
    /// </summary>
    string MessageId { get; }

    /// <summary>Topic the message belongs to.</summary>
    string Topic { get; }

    /// <summary>Partition index within the topic.</summary>
    int Partition { get; }

    /// <summary>Log offset of the message.</summary>
    long Offset { get; }

    /// <summary>Raw bytes of the record batch containing this message.</summary>
    byte[] Body { get; }

    /// <summary>Number of times this message has been delivered (starts at 1).</summary>
    int DeliveryCount { get; }

    /// <summary>Wall-clock time at which visibility timeout expires.</summary>
    DateTimeOffset ExpiresAt { get; }

    /// <summary>
    /// Identifier of the consumer that currently holds this message, or <c>null</c> if unknown.
    /// </summary>
    string? ConsumerId { get; }
}

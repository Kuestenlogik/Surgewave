namespace Kuestenlogik.Surgewave.Core.Queue;

/// <summary>
/// Provides RabbitMQ/SQS-style queue semantics on top of Surgewave's immutable log storage.
/// Messages are never removed from the log — replay remains possible.
/// QueueView only tracks which offsets have been delivered, acknowledged, or need re-delivery.
/// </summary>
/// <remarks>
/// <para>Protocol adapters (AMQP, etc.) interact with this interface rather than with
/// the concrete <c>QueueView</c> in the Broker project, which avoids circular project references.</para>
/// <list type="bullet">
///   <item>Deliver → hides message for a configurable visibility timeout.</item>
///   <item><see cref="Ack"/> → advances committed offset, removes from in-flight.</item>
///   <item><see cref="Nack(string, bool)"/> with requeue=true → message immediately eligible for re-delivery.</item>
///   <item><see cref="Nack(string, bool)"/> with requeue=false / max delivery exceeded → DLQ.</item>
///   <item><see cref="RejectAsync"/> → always routes to DLQ topic.</item>
/// </list>
/// </remarks>
public interface IQueueView : IAsyncDisposable
{
    /// <summary>Gets the number of messages currently in-flight (delivered, awaiting ack).</summary>
    int InFlightCount { get; }

    /// <summary>Gets the highest committed (acked) offset for the given partition, or -1 if none.</summary>
    long CommittedOffset(int partition);

    // -------------------------------------------------------------------------
    // Metrics counters
    // -------------------------------------------------------------------------

    /// <summary>Total number of messages successfully acknowledged since this view was created.</summary>
    long TotalAcked { get; }

    /// <summary>Total number of messages negatively acknowledged (nacked) since this view was created.</summary>
    long TotalNacked { get; }

    /// <summary>Total number of messages permanently rejected (routed to DLQ) since this view was created.</summary>
    long TotalRejected { get; }

    /// <summary>Total number of messages whose visibility timeout expired since this view was created.</summary>
    long TotalExpired { get; }

    /// <summary>Total number of messages that were redelivered (from the re-delivery queue) since this view was created.</summary>
    long TotalRedelivered { get; }

    /// <summary>Total number of messages received (delivered to a consumer) since this view was created.</summary>
    long TotalReceived { get; }

    // -------------------------------------------------------------------------
    // In-flight enumeration
    // -------------------------------------------------------------------------

    /// <summary>
    /// Returns a point-in-time snapshot of all messages currently in-flight (delivered but not yet acknowledged).
    /// </summary>
    IReadOnlyList<IInFlightMessage> GetInFlightMessages();

    /// <summary>
    /// Delivers the next available message(s) from the log to the caller.
    /// Re-delivery queue is checked first; then new messages are read from the log.
    /// Each returned message is placed in-flight with a visibility timeout.
    /// </summary>
    /// <param name="partition">Partition to read from.</param>
    /// <param name="maxMessages">Maximum number of messages to return.</param>
    /// <param name="consumerId">Optional consumer identifier for tracking purposes.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyList<IInFlightMessage>> ReceiveAsync(
        int partition,
        int maxMessages = 1,
        string? consumerId = null,
        CancellationToken ct = default);

    /// <summary>
    /// Acknowledges successful processing of a message.
    /// Removes the message from in-flight tracking and advances the committed offset.
    /// </summary>
    /// <param name="messageId">The <see cref="IInFlightMessage.MessageId"/> to acknowledge.</param>
    /// <returns><c>true</c> if the message was found and acknowledged; <c>false</c> otherwise.</returns>
    bool Ack(string messageId);

    /// <summary>
    /// Negatively acknowledges a message.
    /// When <paramref name="requeue"/> is <see langword="true"/> the message is made
    /// visible again and may be redelivered.
    /// When <paramref name="requeue"/> is <see langword="false"/> the message is silently dropped
    /// (offset advances without redelivery).
    /// </summary>
    /// <param name="messageId">The <see cref="IInFlightMessage.MessageId"/> to nack.</param>
    /// <param name="requeue">Whether to requeue for redelivery.</param>
    /// <returns><c>true</c> if the message was found; <c>false</c> otherwise.</returns>
    bool Nack(string messageId, bool requeue = true);

    /// <summary>
    /// Rejects a message permanently by routing it to the dead-letter topic.
    /// </summary>
    /// <param name="messageId">The <see cref="IInFlightMessage.MessageId"/> to reject.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns><c>true</c> if the message was found and processed; <c>false</c> otherwise.</returns>
    Task<bool> RejectAsync(string messageId, CancellationToken ct = default);

    /// <summary>
    /// Extends the visibility timeout for an in-flight message without changing its
    /// delivery count. Used to back KIP-932 <c>AcknowledgeType=4</c> (Renew): a slow
    /// consumer signals "I am still working on this record, do not redeliver it yet".
    /// Unlike <see cref="Nack(string, bool)"/> with requeue, the message stays
    /// in-flight under the same lease.
    /// </summary>
    /// <param name="messageId">The <see cref="IInFlightMessage.MessageId"/> whose lease should be extended.</param>
    /// <param name="extension">Amount of time to add to the current expiry. <c>null</c> resets to the configured visibility timeout from "now".</param>
    /// <returns><c>true</c> if the message was in-flight and the lease was extended; <c>false</c> otherwise.</returns>
    bool ExtendVisibility(string messageId, TimeSpan? extension = null);
}

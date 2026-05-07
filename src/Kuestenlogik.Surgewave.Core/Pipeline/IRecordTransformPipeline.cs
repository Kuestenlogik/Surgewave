namespace Kuestenlogik.Surgewave.Core.Pipeline;

/// <summary>
/// Hot-path hook for record-level transformations (Redpanda Data Transforms / KIP
/// proposal "Pluggable Topic Authorizer" parity). Implementations are invoked from
/// the broker's produce path on every record batch destined for a topic that has
/// a transform binding in its config.
/// </summary>
/// <remarks>
/// The contract is intentionally narrow so it can plug into the produce hot-path
/// without making the broker depend on a specific WASM runtime:
/// <list type="bullet">
///   <item>Returning the original buffer (or a slice of it) means "no change".</item>
///   <item>Returning a different non-empty buffer means "store these bytes instead".</item>
///   <item>Returning <c>null</c> means "drop the batch silently — succeeded for the
///         producer, no records appended".</item>
/// </list>
/// Implementations must be safe to call concurrently. Throwing is allowed but the
/// broker will surface the error to the producer as a generic Unknown error.
/// </remarks>
public interface IRecordTransformPipeline
{
    /// <summary>
    /// Returns true iff this topic has a transform binding worth invoking. The
    /// broker calls this before <see cref="TransformAsync"/> to short-circuit
    /// the no-binding case without allocating a Task.
    /// </summary>
    bool HasBinding(string topic);

    /// <summary>
    /// Runs the configured transform against the raw record-batch bytes.
    /// </summary>
    /// <param name="topic">Topic the producer is appending to.</param>
    /// <param name="recordBatch">The raw RecordBatch bytes from the producer.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// New record-batch bytes (the transformed payload), or <c>null</c> to drop
    /// the batch. Implementations may return the input slice unchanged.
    /// </returns>
    ValueTask<ReadOnlyMemory<byte>?> TransformAsync(
        string topic,
        ReadOnlyMemory<byte> recordBatch,
        CancellationToken cancellationToken);
}

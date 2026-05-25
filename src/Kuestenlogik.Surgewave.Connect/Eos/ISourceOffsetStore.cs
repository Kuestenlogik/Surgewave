namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Atomic offset storage for exactly-once source connectors.
/// Offsets are committed in the same transaction as the produced messages,
/// ensuring crash recovery resumes from the exact position without duplicates.
/// </summary>
public interface ISourceOffsetStore
{
    /// <summary>
    /// Gets the last committed offset for a source partition.
    /// Returns null if no offset has been committed for this partition.
    /// </summary>
    /// <param name="connectorName">The connector name.</param>
    /// <param name="sourcePartition">The source partition identifier.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The stored offset map, or null if not found.</returns>
    Task<Dictionary<string, string>?> GetOffsetAsync(
        string connectorName,
        string sourcePartition,
        CancellationToken ct = default);

    /// <summary>
    /// Commits an offset atomically with produced messages.
    /// This is the key to exactly-once: the offset is committed in the SAME
    /// transaction as the messages, so both succeed or both fail.
    /// </summary>
    /// <param name="connectorName">The connector name.</param>
    /// <param name="sourcePartition">The source partition identifier.</param>
    /// <param name="offset">The offset map to commit.</param>
    /// <param name="ct">Cancellation token.</param>
    Task CommitOffsetAsync(
        string connectorName,
        string sourcePartition,
        Dictionary<string, string> offset,
        CancellationToken ct = default);

    /// <summary>
    /// Lists all stored offsets for a connector.
    /// </summary>
    /// <param name="connectorName">The connector name.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A dictionary mapping source partition to offset map.</returns>
    Task<IReadOnlyDictionary<string, Dictionary<string, string>>> GetAllOffsetsAsync(
        string connectorName,
        CancellationToken ct = default);

    /// <summary>
    /// Deletes all stored offsets for a connector.
    /// Used when a connector is removed or offsets need to be reset for reprocessing.
    /// </summary>
    /// <param name="connectorName">The connector name.</param>
    /// <param name="ct">Cancellation token.</param>
    Task DeleteOffsetsAsync(
        string connectorName,
        CancellationToken ct = default);
}

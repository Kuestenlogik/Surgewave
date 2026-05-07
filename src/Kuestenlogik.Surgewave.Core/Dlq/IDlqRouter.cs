namespace Kuestenlogik.Surgewave.Core.Dlq;

/// <summary>
/// Routes failed records to Dead Letter Queue topics.
/// </summary>
public interface IDlqRouter
{
    /// <summary>
    /// Route a failed record to its DLQ topic.
    /// </summary>
    /// <param name="record">The DLQ record containing the original message and error context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if successfully routed, false otherwise.</returns>
    Task<bool> RouteAsync(DlqRecord record, CancellationToken cancellationToken = default);

    /// <summary>
    /// Route multiple failed records to DLQ topics.
    /// </summary>
    /// <param name="records">The DLQ records to route.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of records successfully routed.</returns>
    Task<int> RouteBatchAsync(IEnumerable<DlqRecord> records, CancellationToken cancellationToken = default);
}

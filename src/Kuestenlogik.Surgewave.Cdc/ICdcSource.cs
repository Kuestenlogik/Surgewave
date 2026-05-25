namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// Abstraction for a Change Data Capture source.
/// Implementations capture database changes and emit them as <see cref="CdcEvent"/> instances.
/// </summary>
public interface ICdcSource : IAsyncDisposable
{
    /// <summary>
    /// The type of database this source captures changes from (e.g., "PostgreSQL").
    /// </summary>
    string DatabaseType { get; }

    /// <summary>
    /// Captures changes from the database as an async stream.
    /// The stream will continue producing events until cancellation is requested.
    /// </summary>
    /// <param name="ct">Cancellation token to stop capturing.</param>
    /// <returns>An async enumerable of CDC events.</returns>
    IAsyncEnumerable<CdcEvent> CaptureChangesAsync(CancellationToken ct = default);
}

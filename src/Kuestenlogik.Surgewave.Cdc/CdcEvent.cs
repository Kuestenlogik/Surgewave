namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// Represents a single database change event captured from the WAL.
/// </summary>
public sealed record CdcEvent
{
    /// <summary>
    /// The type of change (Insert, Update, Delete, Snapshot).
    /// </summary>
    public required CdcOperation Operation { get; init; }

    /// <summary>
    /// The database schema name (e.g., "public").
    /// </summary>
    public required string Schema { get; init; }

    /// <summary>
    /// The table name where the change occurred.
    /// </summary>
    public required string Table { get; init; }

    /// <summary>
    /// The row state before the change (populated for Update and Delete operations).
    /// Keys are column names, values are column values.
    /// </summary>
    public Dictionary<string, object?>? Before { get; init; }

    /// <summary>
    /// The row state after the change (populated for Insert and Update operations).
    /// Keys are column names, values are column values.
    /// </summary>
    public Dictionary<string, object?>? After { get; init; }

    /// <summary>
    /// The timestamp when the change was committed in the database.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// The PostgreSQL Log Sequence Number (LSN) for this change.
    /// Used for tracking position and resuming after restart.
    /// </summary>
    public long Lsn { get; init; }
}

namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// The type of database change captured by CDC.
/// </summary>
public enum CdcOperation
{
    /// <summary>
    /// A new row was inserted into the table.
    /// </summary>
    Insert,

    /// <summary>
    /// An existing row was updated.
    /// </summary>
    Update,

    /// <summary>
    /// An existing row was deleted.
    /// </summary>
    Delete,

    /// <summary>
    /// A row captured during the initial snapshot phase.
    /// </summary>
    Snapshot
}

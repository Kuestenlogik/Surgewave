namespace Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

/// <summary>
/// Configuration for the materialized view subsystem.
/// Bind via <c>Surgewave:Streams:MaterializedViews</c>.
/// </summary>
public sealed class MaterializedViewOptions
{
    public const string SectionName = "Surgewave:Streams:MaterializedViews";

    /// <summary>
    /// Master switch. When false the refresh service does not start.
    /// Default: true (the subsystem follows the lifetime of whichever protocol
    /// plugin opted in via <c>AddSurgewaveMaterializedViews</c>).
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// How often the refresh loop wakes up to re-evaluate views.
    /// Default: 1 second.
    /// </summary>
    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromSeconds(1);
}

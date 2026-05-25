namespace Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

/// <summary>
/// Static metadata of a materialized view as registered via
/// <c>CREATE MATERIALIZED VIEW name AS SELECT ...</c>.
/// </summary>
/// <param name="Name">View name (case-insensitive identifier).</param>
/// <param name="OriginalSql">The original CREATE statement, kept for diagnostics and re-parsing on restart.</param>
/// <param name="SelectSql">The body SELECT query (without the CREATE wrapper) — used by the refresh loop to re-execute against fresh source rows.</param>
/// <param name="SourceTopics">All topic names referenced in the FROM clause(s) of the SELECT.</param>
/// <param name="KeyColumns">GROUP BY column names (empty for non-aggregating views).</param>
/// <param name="HasAggregation">True when the SELECT contains GROUP BY or aggregate functions.</param>
/// <param name="IfNotExists">True if the view was created with IF NOT EXISTS.</param>
/// <param name="CreatedAt">Timestamp at which the view was registered.</param>
public sealed record ViewDefinition(
    string Name,
    string OriginalSql,
    string SelectSql,
    IReadOnlyList<string> SourceTopics,
    IReadOnlyList<string> KeyColumns,
    bool HasAggregation,
    bool IfNotExists,
    DateTimeOffset CreatedAt);

namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Result of executing a SQL query via the broker REST API.
/// </summary>
public sealed record SqlQueryResult(
    List<string>? Columns,
    List<List<object?>>? Rows,
    int RowCount,
    string? Error);

/// <summary>
/// Information about a continuous SQL query running on the broker.
/// </summary>
public sealed record SqlContinuousQueryInfo(
    string QueryId,
    string Name,
    string Sql,
    string Status,
    long RowsProcessed,
    DateTimeOffset CreatedAt);

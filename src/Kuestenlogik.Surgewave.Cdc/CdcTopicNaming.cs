namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// Utility for generating Surgewave topic names from database schema/table identifiers.
/// </summary>
public static class CdcTopicNaming
{
    /// <summary>
    /// Generates a Surgewave topic name for a given database change event.
    /// </summary>
    /// <param name="config">The CDC configuration containing topic prefix and schema settings.</param>
    /// <param name="schema">The database schema name (e.g., "public").</param>
    /// <param name="table">The table name (e.g., "orders").</param>
    /// <returns>The fully qualified Surgewave topic name.</returns>
    public static string GetTopicName(CdcConfig config, string schema, string table)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);

        var prefix = config.TopicPrefix ?? "";

        if (config.IncludeSchema && !string.IsNullOrWhiteSpace(schema))
        {
            return $"{prefix}{schema}.{table}";
        }

        return $"{prefix}{table}";
    }

    /// <summary>
    /// Generates a message key from the primary key values of a row.
    /// </summary>
    /// <param name="row">The row data containing column values.</param>
    /// <param name="primaryKeyColumns">The names of primary key columns. If empty, returns null.</param>
    /// <returns>A JSON-serialized key string, or null if no primary key columns are specified.</returns>
    public static string? GetMessageKey(Dictionary<string, object?>? row, IReadOnlyList<string>? primaryKeyColumns)
    {
        if (row is null || primaryKeyColumns is null || primaryKeyColumns.Count == 0)
            return null;

        if (primaryKeyColumns.Count == 1)
        {
            // Single-column PK: use the value directly
            return row.TryGetValue(primaryKeyColumns[0], out var value)
                ? value?.ToString()
                : null;
        }

        // Multi-column PK: serialize as JSON object
        var keyParts = new Dictionary<string, object?>();
        foreach (var col in primaryKeyColumns)
        {
            if (row.TryGetValue(col, out var value))
            {
                keyParts[col] = value;
            }
        }

        return System.Text.Json.JsonSerializer.Serialize(keyParts);
    }
}

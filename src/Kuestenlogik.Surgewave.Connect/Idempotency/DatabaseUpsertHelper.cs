using System.Text;

namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// Helper class for generating database-specific upsert SQL statements.
/// Supports common databases like PostgreSQL, MySQL, SQLite, and SQL Server.
/// </summary>
public static class DatabaseUpsertHelper
{
    /// <summary>
    /// Database type for SQL generation.
    /// </summary>
    public enum DatabaseType
    {
        PostgreSQL,
        MySQL,
        SQLite,
        SqlServer
    }

    /// <summary>
    /// Generates an upsert SQL statement for the specified database.
    /// </summary>
    /// <param name="dbType">The target database type.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="keyColumns">The primary key column names.</param>
    /// <param name="valueColumns">The non-key column names.</param>
    /// <param name="strategy">The upsert strategy.</param>
    /// <param name="config">Optional upsert configuration.</param>
    /// <returns>A SQL statement for upsert operations.</returns>
    public static string GenerateUpsertSql(
        DatabaseType dbType,
        string tableName,
        IReadOnlyList<string> keyColumns,
        IReadOnlyList<string> valueColumns,
        UpsertStrategy strategy = UpsertStrategy.InsertOrUpdate,
        UpsertConfig? config = null)
    {
        return dbType switch
        {
            DatabaseType.PostgreSQL => GeneratePostgreSqlUpsert(tableName, keyColumns, valueColumns, strategy, config),
            DatabaseType.MySQL => GenerateMySqlUpsert(tableName, keyColumns, valueColumns, strategy, config),
            DatabaseType.SQLite => GenerateSqliteUpsert(tableName, keyColumns, valueColumns, strategy, config),
            DatabaseType.SqlServer => GenerateSqlServerUpsert(tableName, keyColumns, valueColumns, strategy, config),
            _ => throw new ArgumentOutOfRangeException(nameof(dbType))
        };
    }

    private static string GeneratePostgreSqlUpsert(
        string tableName,
        IReadOnlyList<string> keyColumns,
        IReadOnlyList<string> valueColumns,
        UpsertStrategy strategy,
        UpsertConfig? config)
    {
        var allColumns = keyColumns.Concat(valueColumns).ToList();
        var sb = new StringBuilder();

        sb.Append($"INSERT INTO {tableName} (");
        sb.Append(string.Join(", ", allColumns));

        if (config?.TrackLastModified == true)
            sb.Append($", {config.LastModifiedField}");

        sb.Append(") VALUES (");
        sb.Append(string.Join(", ", allColumns.Select((_, i) => $"@p{i}")));

        if (config?.TrackLastModified == true)
            sb.Append(", CURRENT_TIMESTAMP");

        sb.Append(')');

        switch (strategy)
        {
            case UpsertStrategy.InsertOrUpdate:
                sb.Append($" ON CONFLICT ({string.Join(", ", keyColumns)}) DO UPDATE SET ");
                var updates = valueColumns.Select((col, i) => $"{col} = EXCLUDED.{col}").ToList();
                if (config?.TrackLastModified == true)
                    updates.Add($"{config.LastModifiedField} = CURRENT_TIMESTAMP");
                sb.Append(string.Join(", ", updates));
                break;

            case UpsertStrategy.InsertOrSkip:
                sb.Append($" ON CONFLICT ({string.Join(", ", keyColumns)}) DO NOTHING");
                break;

            case UpsertStrategy.InsertOrUpdateIfNewer when config?.TimestampField != null:
                sb.Append($" ON CONFLICT ({string.Join(", ", keyColumns)}) DO UPDATE SET ");
                updates = valueColumns.Select(col => $"{col} = EXCLUDED.{col}").ToList();
                if (config.TrackLastModified)
                    updates.Add($"{config.LastModifiedField} = CURRENT_TIMESTAMP");
                sb.Append(string.Join(", ", updates));
                sb.Append($" WHERE {tableName}.{config.TimestampField} < EXCLUDED.{config.TimestampField}");
                break;

            case UpsertStrategy.InsertOrFail:
                // No conflict clause - will fail on duplicate
                break;

            default:
                sb.Append($" ON CONFLICT ({string.Join(", ", keyColumns)}) DO UPDATE SET ");
                sb.Append(string.Join(", ", valueColumns.Select(col => $"{col} = EXCLUDED.{col}")));
                break;
        }

        return sb.ToString();
    }

    private static string GenerateMySqlUpsert(
        string tableName,
        IReadOnlyList<string> keyColumns,
        IReadOnlyList<string> valueColumns,
        UpsertStrategy strategy,
        UpsertConfig? config)
    {
        var allColumns = keyColumns.Concat(valueColumns).ToList();
        var sb = new StringBuilder();

        if (strategy == UpsertStrategy.Replace)
        {
            sb.Append($"REPLACE INTO {tableName} (");
        }
        else if (strategy == UpsertStrategy.InsertOrSkip)
        {
            sb.Append($"INSERT IGNORE INTO {tableName} (");
        }
        else
        {
            sb.Append($"INSERT INTO {tableName} (");
        }

        sb.Append(string.Join(", ", allColumns));

        if (config?.TrackLastModified == true)
            sb.Append($", {config.LastModifiedField}");

        sb.Append(") VALUES (");
        sb.Append(string.Join(", ", allColumns.Select((_, i) => $"@p{i}")));

        if (config?.TrackLastModified == true)
            sb.Append(", NOW()");

        sb.Append(')');

        if (strategy == UpsertStrategy.InsertOrUpdate)
        {
            sb.Append(" ON DUPLICATE KEY UPDATE ");
            var updates = valueColumns.Select(col => $"{col} = VALUES({col})").ToList();
            if (config?.TrackLastModified == true)
                updates.Add($"{config.LastModifiedField} = NOW()");
            sb.Append(string.Join(", ", updates));
        }

        return sb.ToString();
    }

    private static string GenerateSqliteUpsert(
        string tableName,
        IReadOnlyList<string> keyColumns,
        IReadOnlyList<string> valueColumns,
        UpsertStrategy strategy,
        UpsertConfig? config)
    {
        var allColumns = keyColumns.Concat(valueColumns).ToList();
        var sb = new StringBuilder();

        if (strategy == UpsertStrategy.Replace)
        {
            sb.Append($"INSERT OR REPLACE INTO {tableName} (");
        }
        else if (strategy == UpsertStrategy.InsertOrSkip)
        {
            sb.Append($"INSERT OR IGNORE INTO {tableName} (");
        }
        else
        {
            sb.Append($"INSERT INTO {tableName} (");
        }

        sb.Append(string.Join(", ", allColumns));

        if (config?.TrackLastModified == true)
            sb.Append($", {config.LastModifiedField}");

        sb.Append(") VALUES (");
        sb.Append(string.Join(", ", allColumns.Select((_, i) => $"@p{i}")));

        if (config?.TrackLastModified == true)
            sb.Append(", datetime('now')");

        sb.Append(')');

        if (strategy == UpsertStrategy.InsertOrUpdate)
        {
            sb.Append($" ON CONFLICT({string.Join(", ", keyColumns)}) DO UPDATE SET ");
            var updates = valueColumns.Select(col => $"{col} = excluded.{col}").ToList();
            if (config?.TrackLastModified == true)
                updates.Add($"{config.LastModifiedField} = datetime('now')");
            sb.Append(string.Join(", ", updates));
        }

        return sb.ToString();
    }

    private static string GenerateSqlServerUpsert(
        string tableName,
        IReadOnlyList<string> keyColumns,
        IReadOnlyList<string> valueColumns,
        UpsertStrategy strategy,
        UpsertConfig? config)
    {
        var allColumns = keyColumns.Concat(valueColumns).ToList();
        var sb = new StringBuilder();

        // SQL Server uses MERGE statement
        sb.AppendLine($"MERGE INTO {tableName} AS target");
        sb.AppendLine("USING (SELECT ");
        sb.Append(string.Join(", ", allColumns.Select((col, i) => $"@p{i} AS {col}")));
        sb.AppendLine(") AS source");
        sb.Append("ON ");
        sb.AppendLine(string.Join(" AND ", keyColumns.Select(col => $"target.{col} = source.{col}")));

        switch (strategy)
        {
            case UpsertStrategy.InsertOrUpdate:
                sb.Append("WHEN MATCHED THEN UPDATE SET ");
                var updates = valueColumns.Select(col => $"target.{col} = source.{col}").ToList();
                if (config?.TrackLastModified == true)
                    updates.Add($"target.{config.LastModifiedField} = GETUTCDATE()");
                sb.AppendLine(string.Join(", ", updates));
                break;

            case UpsertStrategy.InsertOrSkip:
                // No WHEN MATCHED clause - just insert
                break;
        }

        sb.Append("WHEN NOT MATCHED THEN INSERT (");
        sb.Append(string.Join(", ", allColumns));
        if (config?.TrackLastModified == true)
            sb.Append($", {config.LastModifiedField}");
        sb.Append(") VALUES (");
        sb.Append(string.Join(", ", allColumns.Select(col => $"source.{col}")));
        if (config?.TrackLastModified == true)
            sb.Append(", GETUTCDATE()");
        sb.AppendLine(");");

        return sb.ToString();
    }
}

namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// Strategies for handling upsert operations in sink connectors.
/// </summary>
public enum UpsertStrategy
{
    /// <summary>
    /// Insert if not exists, update if exists (INSERT ... ON CONFLICT UPDATE).
    /// Most common strategy for database sinks.
    /// </summary>
    InsertOrUpdate,

    /// <summary>
    /// Insert if not exists, skip if exists (INSERT ... ON CONFLICT DO NOTHING).
    /// Use when you only want the first occurrence of a record.
    /// </summary>
    InsertOrSkip,

    /// <summary>
    /// Insert if not exists, fail if exists.
    /// Use when duplicates should raise an error.
    /// </summary>
    InsertOrFail,

    /// <summary>
    /// Always replace the entire record.
    /// Use with caution - may lose concurrent updates.
    /// </summary>
    Replace,

    /// <summary>
    /// Insert if not exists, update only if newer (based on timestamp or version).
    /// Handles out-of-order delivery.
    /// </summary>
    InsertOrUpdateIfNewer
}

/// <summary>
/// Configuration for upsert behavior.
/// </summary>
public sealed record UpsertConfig
{
    /// <summary>
    /// The upsert strategy to use.
    /// </summary>
    public UpsertStrategy Strategy { get; init; } = UpsertStrategy.InsertOrUpdate;

    /// <summary>
    /// Fields that form the primary key for upsert operations.
    /// </summary>
    public IReadOnlyList<string> KeyFields { get; init; } = [];

    /// <summary>
    /// Field name for timestamp-based versioning (for InsertOrUpdateIfNewer strategy).
    /// </summary>
    public string? TimestampField { get; init; }

    /// <summary>
    /// Field name for version-based optimistic locking.
    /// </summary>
    public string? VersionField { get; init; }

    /// <summary>
    /// Whether to track last-modified timestamp automatically.
    /// </summary>
    public bool TrackLastModified { get; init; } = true;

    /// <summary>
    /// Name of the last-modified timestamp field.
    /// </summary>
    public string LastModifiedField { get; init; } = "_surgewave_last_modified";
}

/// <summary>
/// Result of an upsert operation.
/// </summary>
public sealed record UpsertResult
{
    /// <summary>
    /// Whether the operation succeeded.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// The type of operation performed.
    /// </summary>
    public required UpsertOperation Operation { get; init; }

    /// <summary>
    /// Number of rows affected.
    /// </summary>
    public int RowsAffected { get; init; }

    /// <summary>
    /// Error message if failed.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// Creates a successful insert result.
    /// </summary>
    public static UpsertResult Inserted(int rows = 1) => new()
    {
        Success = true,
        Operation = UpsertOperation.Inserted,
        RowsAffected = rows
    };

    /// <summary>
    /// Creates a successful update result.
    /// </summary>
    public static UpsertResult Updated(int rows = 1) => new()
    {
        Success = true,
        Operation = UpsertOperation.Updated,
        RowsAffected = rows
    };

    /// <summary>
    /// Creates a skipped result.
    /// </summary>
    public static UpsertResult Skipped() => new()
    {
        Success = true,
        Operation = UpsertOperation.Skipped,
        RowsAffected = 0
    };

    /// <summary>
    /// Creates a failed result.
    /// </summary>
    public static UpsertResult Failed(string error) => new()
    {
        Success = false,
        Operation = UpsertOperation.Failed,
        Error = error
    };
}

/// <summary>
/// Type of upsert operation that was performed.
/// </summary>
public enum UpsertOperation
{
    /// <summary>
    /// A new row was inserted.
    /// </summary>
    Inserted,

    /// <summary>
    /// An existing row was updated.
    /// </summary>
    Updated,

    /// <summary>
    /// The operation was skipped (duplicate, older version, etc.).
    /// </summary>
    Skipped,

    /// <summary>
    /// The operation failed.
    /// </summary>
    Failed
}

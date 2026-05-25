namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// Configuration for zero-downtime schema migration.
/// Bound from appsettings.json under "Surgewave:SchemaMigration".
/// </summary>
public sealed class SchemaMigrationConfig
{
    /// <summary>
    /// Enable zero-downtime schema migration.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Automatically migrate old messages to the consumer's expected schema version on read.
    /// </summary>
    public bool AutoMigrateOnRead { get; set; } = true;

    /// <summary>
    /// Automatically migrate produced messages to the latest schema version on write.
    /// </summary>
    public bool AutoMigrateOnWrite { get; set; } = false;

    /// <summary>
    /// Strategy for handling fields present in the target schema but missing from the source message.
    /// </summary>
    public MissingFieldStrategy MissingFieldStrategy { get; set; } = MissingFieldStrategy.UseDefault;

    /// <summary>
    /// Strategy for handling fields present in the source message but not in the target schema.
    /// </summary>
    public ExtraFieldStrategy ExtraFieldStrategy { get; set; } = ExtraFieldStrategy.Ignore;

    /// <summary>
    /// Strategy for handling type mismatches between source and target schemas.
    /// </summary>
    public TypeMismatchStrategy TypeMismatchStrategy { get; set; } = TypeMismatchStrategy.Coerce;

    /// <summary>
    /// Maximum number of cached compiled migrators (LRU eviction when exceeded).
    /// </summary>
    public int MaxCachedMigrators { get; set; } = 100;
}

/// <summary>
/// Strategy for fields that exist in the target schema but are missing from the source message.
/// </summary>
public enum MissingFieldStrategy
{
    /// <summary>Use the JSON Schema default or the type's default value (0, "", false, null).</summary>
    UseDefault,

    /// <summary>Use null for missing fields (requires nullable target type).</summary>
    UseNull,

    /// <summary>Fail the migration if any required field is missing.</summary>
    Fail
}

/// <summary>
/// Strategy for fields that exist in the source message but are not in the target schema.
/// </summary>
public enum ExtraFieldStrategy
{
    /// <summary>Silently drop extra fields not in the target schema.</summary>
    Ignore,

    /// <summary>Include extra fields in the output even though they are not in the target schema.</summary>
    Include,

    /// <summary>Fail the migration if extra fields are present.</summary>
    Fail
}

/// <summary>
/// Strategy for handling type mismatches between source and target field types.
/// </summary>
public enum TypeMismatchStrategy
{
    /// <summary>Attempt automatic type coercion (int to string, string to int, etc.).</summary>
    Coerce,

    /// <summary>Use the target type's default value when coercion would be needed.</summary>
    UseDefault,

    /// <summary>Fail the migration if any type mismatch is detected.</summary>
    Fail
}

namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// Describes a single field-level change between two schema versions.
/// </summary>
public sealed class FieldChange
{
    /// <summary>
    /// The name of the field that changed.
    /// For renames, this is the new field name.
    /// </summary>
    public required string FieldName { get; init; }

    /// <summary>
    /// The kind of change (Added, Removed, TypeChanged, etc.).
    /// </summary>
    public required FieldChangeType Type { get; init; }

    /// <summary>
    /// The JSON Schema type of the field in the old schema (null for Added fields).
    /// </summary>
    public string? OldType { get; init; }

    /// <summary>
    /// The JSON Schema type of the field in the new schema (null for Removed fields).
    /// </summary>
    public string? NewType { get; init; }

    /// <summary>
    /// For renames: the previous field name.
    /// </summary>
    public string? OldFieldName { get; init; }

    /// <summary>
    /// Whether the field has a default value in the new schema.
    /// </summary>
    public bool HasDefault { get; init; }

    /// <summary>
    /// The default value (as string) if known.
    /// </summary>
    public string? DefaultValue { get; init; }

    /// <summary>
    /// The breaking level of this individual field change.
    /// </summary>
    public BreakingLevel Breaking { get; init; }
}

namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// Describes a single field-level transformation that must be applied during migration.
/// </summary>
public sealed class FieldTransform
{
    /// <summary>
    /// The JSON path to the field (e.g., "name", "address.city").
    /// </summary>
    public required string FieldPath { get; init; }

    /// <summary>
    /// The kind of transformation to apply.
    /// </summary>
    public required FieldTransformType Type { get; init; }

    /// <summary>
    /// The default value to use for AddWithDefault transforms (JSON-encoded).
    /// </summary>
    public string? DefaultValue { get; init; }

    /// <summary>
    /// The JSON Schema type of the field in the source schema.
    /// </summary>
    public string? SourceType { get; init; }

    /// <summary>
    /// The JSON Schema type of the field in the target schema.
    /// </summary>
    public string? TargetType { get; init; }

    /// <summary>
    /// For rename transforms: the old field name.
    /// </summary>
    public string? OldFieldName { get; init; }
}

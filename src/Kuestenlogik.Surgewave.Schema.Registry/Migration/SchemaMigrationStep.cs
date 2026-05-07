namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// A single step in a multi-version migration path (e.g., v1 to v2).
/// Contains the transforms needed to convert a message between two adjacent schema versions.
/// </summary>
public sealed class SchemaMigrationStep
{
    /// <summary>
    /// The source schema version.
    /// </summary>
    public required int FromVersion { get; init; }

    /// <summary>
    /// The target schema version.
    /// </summary>
    public required int ToVersion { get; init; }

    /// <summary>
    /// The field-level transforms to apply for this step.
    /// </summary>
    public required List<FieldTransform> Transforms { get; init; }
}

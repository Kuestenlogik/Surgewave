namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// The kind of transformation applied to a field during schema migration.
/// </summary>
public enum FieldTransformType
{
    /// <summary>Add a field with a default value (field exists in target but not source).</summary>
    AddWithDefault,

    /// <summary>Remove a field (field exists in source but not target).</summary>
    Remove,

    /// <summary>Rename a field (detected heuristically).</summary>
    Rename,

    /// <summary>Change the type of a field (e.g., integer to string).</summary>
    ChangeType,

    /// <summary>Make a required field nullable.</summary>
    MakeNullable,

    /// <summary>Make an optional field required.</summary>
    MakeRequired
}

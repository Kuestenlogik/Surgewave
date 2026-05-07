namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// The type of schema change detected between two versions.
/// </summary>
public enum SchemaChangeType
{
    /// <summary>A single field was added.</summary>
    FieldAdded,

    /// <summary>A single field was removed.</summary>
    FieldRemoved,

    /// <summary>A single field had its type changed.</summary>
    FieldTypeChanged,

    /// <summary>A field was renamed (heuristic: same type, old removed + new added).</summary>
    FieldRenamed,

    /// <summary>Multiple different kinds of changes were detected.</summary>
    Multiple
}

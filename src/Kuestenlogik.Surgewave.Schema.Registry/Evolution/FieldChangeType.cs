namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// The kind of change that happened to an individual field.
/// </summary>
public enum FieldChangeType
{
    /// <summary>Field was added to the schema.</summary>
    Added,

    /// <summary>Field was removed from the schema.</summary>
    Removed,

    /// <summary>Field type was changed (e.g., integer to string).</summary>
    TypeChanged,

    /// <summary>Field was renamed (detected heuristically).</summary>
    Renamed,

    /// <summary>Field was made nullable (was required, now optional).</summary>
    MadeNullable,

    /// <summary>Field was made required (was optional, now required).</summary>
    MadeRequired
}

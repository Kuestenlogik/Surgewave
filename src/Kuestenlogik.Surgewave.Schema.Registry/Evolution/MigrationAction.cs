namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// The kind of action a consumer must take to handle a schema change.
/// </summary>
public enum MigrationAction
{
    /// <summary>No action required — the change is transparent.</summary>
    NoActionNeeded,

    /// <summary>Update the C# model class to reflect new/changed fields.</summary>
    UpdateModel,

    /// <summary>Add a default value for a new field.</summary>
    AddDefault,

    /// <summary>Add null-check logic for a newly nullable field.</summary>
    HandleNull,

    /// <summary>Update deserialization code for type or structure changes.</summary>
    UpdateDeserializer,

    /// <summary>Update query or filter logic that references changed fields.</summary>
    UpdateQuery
}

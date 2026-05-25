namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// Indicates the severity of a schema change with respect to existing consumers.
/// </summary>
public enum BreakingLevel
{
    /// <summary>Non-breaking change — existing consumers are unaffected.</summary>
    None,

    /// <summary>Minor change — new consumers need awareness, but old data still works.</summary>
    Minor,

    /// <summary>Major/breaking change — existing consumers will fail without a code update.</summary>
    Major
}

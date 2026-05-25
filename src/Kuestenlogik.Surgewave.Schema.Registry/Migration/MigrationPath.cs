namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// Describes the full migration path between two schema versions for a subject,
/// including all intermediate steps and their transforms.
/// </summary>
public sealed class MigrationPath
{
    /// <summary>
    /// The schema subject name.
    /// </summary>
    public required string Subject { get; init; }

    /// <summary>
    /// The starting schema version.
    /// </summary>
    public required int FromVersion { get; init; }

    /// <summary>
    /// The target schema version.
    /// </summary>
    public required int ToVersion { get; init; }

    /// <summary>
    /// The ordered list of migration steps (v1->v2, v2->v3, etc.).
    /// </summary>
    public required List<SchemaMigrationStep> Steps { get; init; }

    /// <summary>
    /// Whether this is a direct migration (single step) or multi-step.
    /// </summary>
    public bool IsDirectMigration => Steps.Count <= 1;

    /// <summary>
    /// Total number of field transforms across all steps.
    /// </summary>
    public int TotalTransformCount
    {
        get
        {
            var count = 0;
            foreach (var step in Steps)
            {
                count += step.Transforms.Count;
            }
            return count;
        }
    }
}

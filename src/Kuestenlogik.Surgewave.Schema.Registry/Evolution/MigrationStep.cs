namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// A single step in a consumer migration plan.
/// </summary>
public sealed class MigrationStep
{
    /// <summary>
    /// The order of this step in the migration sequence.
    /// </summary>
    public required int Order { get; init; }

    /// <summary>
    /// Human-readable description of what this step does.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    /// The category of action for this step.
    /// </summary>
    public required MigrationAction Action { get; init; }

    /// <summary>
    /// Optional C# code snippet for this step.
    /// </summary>
    public string? CodeSnippet { get; init; }
}

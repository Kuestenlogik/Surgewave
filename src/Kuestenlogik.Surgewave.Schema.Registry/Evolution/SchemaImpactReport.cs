namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// A comprehensive report describing the impact of a schema change on consumers,
/// including migration steps and generated code.
/// </summary>
public sealed record SchemaImpactReport
{
    /// <summary>
    /// The schema change this report is about.
    /// </summary>
    public required SchemaChange Change { get; init; }

    /// <summary>
    /// Human-readable summary of the changes.
    /// </summary>
    public required string Summary { get; init; }

    /// <summary>
    /// Consumer groups that consume from topics using this subject.
    /// </summary>
    public required List<string> AffectedConsumers { get; init; }

    /// <summary>
    /// Ordered migration steps for consumer code.
    /// </summary>
    public required List<MigrationStep> MigrationSteps { get; init; }

    /// <summary>
    /// Generated C# migration code (model class + consumer update).
    /// </summary>
    public required string GeneratedCode { get; init; }

    /// <summary>
    /// Optional LLM-generated natural language explanation.
    /// Populated only when an LLM backend is configured.
    /// </summary>
    public string? LlmExplanation { get; init; }
}
